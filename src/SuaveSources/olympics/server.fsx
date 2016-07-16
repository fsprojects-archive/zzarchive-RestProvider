#I "../../../packages/samples"
#r "System.Xml.Linq.dll"
#r "FSharp.Data/lib/net40/FSharp.Data.dll"
#r "Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#r "Suave/lib/net40/Suave.dll"
open System
open System.IO
open FSharp.Data
open System.Collections.Generic
open Newtonsoft.Json

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

let serializer = JsonSerializer.Create()

let toJson value = 
  let sb = System.Text.StringBuilder()
  use tw = new System.IO.StringWriter(sb)
  serializer.Serialize(tw, value)
  sb.ToString() 

let formatPairSeq kf data =
  let json = 
    data 
    |> Seq.map (fun (k, v) -> JsonValue.Array [| kf k; JsonValue.Float v |])
    |> Array.ofSeq
    |> JsonValue.Array
  json.ToString()


// ----------------------------------------------------------------------------
// Server
// ----------------------------------------------------------------------------

open Suave
open Suave.Filters
open Suave.Operators

type RecordField = { name:string; ``type``:obj }
type GenericType = { name:string; ``params``:obj[] }
type RecordType = { name:string (* = record *); fields:RecordField[] }
type TypePrimitive = { kind:string; ``type``:obj; endpoint:string }
type TypeNested = { kind:string; endpoint:string }
type Member = { name:string; returns:obj; trace:string[] }

let memberPath s f = 
  path s >=> request (fun _ -> f() |> Array.ofSeq |> toJson |> Successful.OK)

let memberPathf fmt f = 
  pathScan fmt (fun b -> f b |> Array.ofSeq |> toJson |> Successful.OK)

let (|Lookup|_|) k (dict:IDictionary<_,_>) =
  match dict.TryGetValue k with
  | true, v -> Some v
  | _ -> None

type Facet<'T> = 
  | Filter of ('T -> option<string>)
  | Choice of seq<string * string * Facet<'T>>

module Data = 
  let [<Literal>] Root = __SOURCE_DIRECTORY__ + "/medals-merged.csv"
  type Medals = CsvProvider<Root>
  let olympics = Medals.GetSample().Rows

  type Codes = FSharp.Data.HtmlProvider<"http://www.topendsports.com/events/summer/countries/country-codes.htm">
  let countries = 
    [ yield "SRB", "Serbia"
      for r in Codes.GetSample().Tables.``3-Digit Country Codes``.Rows do 
        yield r.Code, r.Country ] |> dict

  let sports = 
    olympics 
    |> Seq.map (fun o -> o.Sport) 
    |> Seq.distinct
    |> Seq.mapi (fun i s -> sprintf "sport-%d" i, s)
    |> dict

  let nocs = 
    olympics 
    |> Seq.map (fun o -> o.NOC) 
    |> Seq.distinct
    |> Seq.mapi (fun i s -> sprintf "noc-%d" i, s)
    |> dict
  
  let facets : list<string * Facet<Medals.Row>> = 
    [ "city", Filter(fun r -> Some(sprintf "%s (%d)" r.City r.Edition))
      "medal", Filter(fun r -> Some(r.Medal))
      "gender", Filter(fun r -> Some(r.Gender))
      "country", Filter(fun r -> Some(countries.[r.NOC]))
      "athlete", Choice [ 
        for (KeyValue(k,v)) in nocs -> 
          k, countries.[v], Filter(fun r -> if r.NOC = v then Some(r.Athlete) else None) ]
      "sport", Choice [ 
        for (KeyValue(k,v)) in sports -> 
          k, v, Filter(fun r -> if r.Sport = v then Some(r.Event) else None) ] ]

  let facetsLookup = dict facets

  let rec findFilter path facet = 
    match path, facet with
    | [], f -> f
    | p::ps, Choice choices -> 
        match choices |> Seq.tryPick (fun (k, _, f) -> if p = k  then Some(findFilter ps f) else None) with
        | Some f -> f
        | None -> failwithf "Could not find filter '%s' in choices '%A'" p [ for c, _, _ in choices -> c ]
    | _ -> failwith "Mismatching filter"

  let findFacet prefix = 
    match prefix with
    | [] -> failwith "Empty facet"
    | f::fs -> findFilter fs facetsLookup.[f]
    
    
let (|SplitBy|_|) k l = 
  let rec loop acc = function
    | x::xs when x = k -> Some(List.rev acc, xs)
    | x::xs -> loop (x::acc) xs
    | [] -> None
  loop [] l

let (</>) (a:string) (b:string) = 
  let a, b = a.Trim('/'), b.Trim('/')
  if a = "" then "/" + b 
  elif b = "" then "/" + a
  else "/" + a + "/" + b

let app =
  request (fun r ->
    let selected = r.url.LocalPath.Split([|'/'|], StringSplitOptions.RemoveEmptyEntries) |> List.ofSeq
    match selected with
    | ["data"] ->
        let data = (Utils.ASCII.toString r.rawForm)
        let constraints = 
          data.Split([|'&'|], StringSplitOptions.RemoveEmptyEntries) 
          |> Array.map (fun kvp -> 
              let kv = kvp.Split('=') in 
              Data.findFacet (List.ofSeq (kv.[0].Split('/'))), System.Web.HttpUtility.UrlDecode(kv.[1]))
        
        Data.olympics
        |> Seq.filter (fun r ->
            constraints |> Seq.forall (fun (f, v) -> 
              match f with 
              | Filter f -> f r = Some v
              | _ -> false))
        |> Seq.map (fun r -> 
            let flds = Microsoft.FSharp.Reflection.FSharpValue.GetTupleFields(r) |> List.ofSeq
            let headers = Data.Medals.GetSample().Headers.Value
            Seq.zip flds headers 
            |> Seq.map (fun (fld, hdr) ->
                hdr, 
                if hdr = "Edition" then JsonValue.Number(decimal (fld :?> int))
                elif hdr = "NOC" then JsonValue.String(Data.countries.[fld :?> string])
                else JsonValue.String(string fld))
            |> Array.ofSeq
            |> JsonValue.Record )
        |> Array.ofSeq
        |> JsonValue.Array
        |> string        
        |> Successful.OK

    | SplitBy "pick" (rest, path) ->
        match Data.findFacet path with
        | Filter f ->
            Data.olympics
            |> Seq.choose f
            |> Seq.distinct
            |> Seq.map (fun value ->
                { name=value; returns= {kind="nested"; endpoint=(List.fold (</>) "" rest) </> List.head path }
                  trace=[| String.concat "/" path + "=" + System.Web.HttpUtility.UrlEncode value |] })
            |> Array.ofSeq |> toJson |> Successful.OK 
        | Choice c ->
            c
            |> Seq.map (fun (k, v, _) ->
                { name=v; returns= {kind="nested"; endpoint= (List.fold (</>) "" selected) </> k }
                  trace=[| |] })
            |> Array.ofSeq |> toJson |> Successful.OK             

    | selected ->
        Data.facets 
        |> Seq.filter (fun (facetKey, _) -> 
            selected |> Seq.forall (fun sel -> not (sel.StartsWith(facetKey))))
        |> Seq.map (fun (facetKey, _) ->
            { name="by " + facetKey; returns= {kind="nested"; endpoint=r.url.LocalPath </> "pick" </> facetKey}
              trace=[| |] })
        |> Seq.append [
            ( let headers = Data.Medals.GetSample().Headers.Value
              let flds = [| for h in headers -> { name=h; ``type``=if h="Edition" then "int" else "string" }  |]
              let typ = { name="seq"; ``params``=[| { name="record"; fields=flds } |] }
              { name="data"; returns= {kind="primitive"; ``type``=typ; endpoint="/data"}
                trace=[| |] } ) ]
        |> Array.ofSeq |> toJson |> Successful.OK )