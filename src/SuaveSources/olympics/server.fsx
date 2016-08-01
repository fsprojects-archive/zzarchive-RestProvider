#nowarn "1104"
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

type ThingSchema = { ``@context``:string; ``@type``:string; name:string; }
type CollectionSchema = { ``@type``:string; name:string }
type AddActionSchema = { ``@context``:string; ``@type``:string; targetCollection:CollectionSchema }
type CreateActionSchema = { ``@context``:string; ``@type``:string; result:CollectionSchema }

type RecordField = { name:string; ``type``:obj }
type GenericType = { name:string; ``params``:obj[] }
type RecordType = { name:string (* = record *); fields:RecordField[] }
type TypePrimitive = { kind:string; ``type``:obj; endpoint:string }
type TypeNested = { kind:string; endpoint:string }
type Member = { name:string; returns:obj; trace:string[]; schema:obj }


let memberPath s f = 
  path s >=> request (fun _ -> f() |> Array.ofSeq |> toJson |> Successful.OK)

let memberPathf fmt f = 
  pathScan fmt (fun b -> f b |> Array.ofSeq |> toJson |> Successful.OK)

let (|Lookup|_|) k (dict:IDictionary<_,_>) =
  match dict.TryGetValue k with
  | true, v -> Some v
  | _ -> None


type Facet<'T> = 
  | Filter of multichoice:bool * ('T -> option<string * ThingSchema>)
  | Choice of seq<string * string * ThingSchema * Facet<'T>>

module Data = 
  let [<Literal>] Root = __SOURCE_DIRECTORY__ + "/medals-expanded.csv"
  type Medals = CsvProvider<Root, Schema="Gold=int, Silver=int, Bronze=int">
  let olympics = Medals.GetSample().Rows
  
  // http://www.topendsports.com/events/summer/countries/country-codes.htm
  type Codes = FSharp.Data.HtmlProvider<const(__SOURCE_DIRECTORY__ + "/countrycodes.html")>
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
  
  let makeThingSchema kind name =
    { ``@context`` = "http://schema.org/"; ``@type`` = kind; name = name }
  let makeCreateSchema name =
    let col = { CollectionSchema.``@type`` = "ItemList"; name=name }
    { ``@context`` = "http://schema.org/"; ``@type`` = "CreateAction"; result = col }
  let makeAddSchema name =
    let col = { CollectionSchema.``@type`` = "ItemList"; name=name }
    { ``@context`` = "http://schema.org/"; ``@type`` = "AddAction"; targetCollection = col }
  let noSchema = Unchecked.defaultof<ThingSchema>

  let facets : list<string * Facet<Medals.Row>> = 
    [ // Single-choice 
      yield "city", Filter(false, fun r -> Some(sprintf "%s (%d)" r.City r.Edition, makeThingSchema "City" r.City))
      yield "medal", Filter(false, fun r -> Some(r.Medal, noSchema))
      yield "gender", Filter(false, fun r -> Some(r.Gender, noSchema))
      yield "country", Filter(false, fun r -> let c = countries.[r.NOC] in Some(c, makeThingSchema "Country" c))

      // Multi-choice
      yield "cities", Filter(true, fun r -> Some(sprintf "%s (%d)" r.City r.Edition, makeThingSchema "City" r.City))
      yield "medals", Filter(true, fun r -> Some(r.Medal, noSchema))
      yield "countries", Filter(true, fun r -> let c = countries.[r.NOC] in Some(c, makeThingSchema "Country" c))

      // Multi-level facet with/without multi-choice
      let athleteChoice multi =  
        [ for (KeyValue(k,v)) in nocs -> 
            let c = countries.[v]
            k, c, makeThingSchema "Country" c, Filter(multi, fun (r:Medals.Row) -> 
              if r.NOC = v then Some(r.Athlete, makeThingSchema "Person" r.Athlete) else None) ]
      let sportChoice multi = 
        [ for (KeyValue(k,v)) in sports -> 
            k, v, makeThingSchema "SportsEvent" v, Filter(multi, fun (r:Medals.Row) ->  
              if r.Sport = v then Some(r.Event, makeThingSchema "SportsEvent" r.Event) else None) ]

      yield "athlete", Choice(athleteChoice false)
      yield "athletes", Choice(athleteChoice true)
      yield "sport", Choice (sportChoice false)
      yield "sports", Choice (sportChoice true) ]


  let facetsLookup = dict facets

  let rec findFilter path facet = 
    match path, facet with
    | [], f -> f
    | p::ps, Choice choices -> 
        match choices |> Seq.tryPick (fun (k, _, _, f) -> if p = k  then Some(findFilter ps f) else None) with
        | Some f -> f
        | None -> failwithf "Could not find filter '%s' in choices '%A'" p [ for c, _, _, _ in choices -> c ]
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

let intFields = set ["Edition"; "Gold"; "Silver"; "Bronze"]
let (|Let|) a v = a, v

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
              (List.ofSeq (kv.[0].Split('/'))), System.Web.HttpUtility.UrlDecode(kv.[1]))
          |> Seq.groupBy fst
          |> Seq.map (fun (facet, values) ->
              Data.findFacet facet, set (Seq.map snd values))

        Data.olympics
        |> Seq.filter (fun r ->
            constraints |> Seq.forall (fun (f, vs) -> 
              match f with 
              | Filter(_, f) -> match f r with Some(v, _) -> vs.Contains v | _ -> false
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

    | Let true (nested, SplitBy "and-pick" (rest, path))
    | Let false (nested, SplitBy "pick" (rest, path)) ->
        let prefix = if nested then "or " else ""
        match Data.findFacet path with
        | Filter(multichoice, f) ->
            let options = Data.olympics |> Seq.choose f |> Seq.distinctBy fst
            let andPickTy = 
              { kind="nested"; endpoint=(List.fold (</>) "" rest) </> "and-pick" </> (List.fold (</>) "" path) }
            let thenTy = 
              { kind="nested"; endpoint=(List.fold (</>) "" rest) </> List.head path }

            [ for (value, schema) in options do
                let schema = 
                  if multichoice then Data.makeAddSchema (List.head path) |> box
                  else schema |> box

                let ty = if multichoice then andPickTy else thenTy
                let trace = [| String.concat "/" path + "=" + System.Web.HttpUtility.UrlEncode value |]
                yield { name=prefix + value; schema=schema; returns=ty; trace=trace }
              if multichoice then 
                yield { name="then"; returns=thenTy; schema=Data.noSchema; trace=[| |] } ]
            |> Array.ofSeq |> toJson |> Successful.OK 

        | Choice(c) ->
            c
            |> Seq.map (fun (k, v, s, facet) ->
                let schema = 
                  match facet with
                  | Filter(true, _) -> Data.makeCreateSchema k |> box
                  | _ -> s |> box
                { name=v; returns= {kind="nested"; endpoint= (List.fold (</>) "" selected) </> k }
                  schema=schema; trace=[| |] })
            |> Array.ofSeq |> toJson |> Successful.OK             

    | selected ->
        Data.facets 
        |> Seq.filter (fun (facetKey, _) -> 
            selected |> Seq.forall (fun sel -> not (sel.StartsWith(facetKey))))
        |> Seq.map (fun (facetKey, facet) ->
            let schema = 
              match facet with
              | Filter(true, _) -> Data.makeCreateSchema facetKey |> box
              | _ -> Data.noSchema |> box
            { name="by " + facetKey; returns= {kind="nested"; endpoint=r.url.LocalPath </> "pick" </> facetKey}
              schema=schema; trace=[| |] })
        |> Seq.append [
            ( let headers = Data.Medals.GetSample().Headers.Value
              let flds = [| for h in headers -> { name=h; ``type``=if intFields.Contains h then "int" else "string" }  |]
              let typ = { name="seq"; ``params``=[| { name="record"; fields=flds } |] }
              { name="data"; returns= {kind="primitive"; ``type``=typ; endpoint="/data"}
                schema=Data.noSchema; trace=[| |] } ) ]
        |> Array.ofSeq |> toJson |> Successful.OK )