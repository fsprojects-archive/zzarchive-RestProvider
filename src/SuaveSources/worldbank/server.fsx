#I "../../../packages/samples"
#r "System.Xml.Linq.dll"
#r "FSharp.Data/lib/net40/FSharp.Data.dll"
#r "Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#r "Suave/lib/net40/Suave.dll"
#load "domain.fs"
open System
open System.IO
open FSharp.Data
open System.Collections.Generic
open WorldBank
open WorldBank.Domain
open Newtonsoft.Json

let worldBank = Domain.readCache (__SOURCE_DIRECTORY__ + "/cache")

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

type GenericType = { name:string; ``params``:obj[] }
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

let app =
  choose [ 
    memberPath "/" (fun () ->
      [ { name="byYear"; returns= {kind="nested"; endpoint="/pickYear"}
          trace=[| |] } 
        { name="byCountry"; returns= {kind="nested"; endpoint="/pickCountry"}
          trace=[| |] } ])

    memberPath "/pickCountry" (fun () ->
      [ for (KeyValue(id, country)) in worldBank.Countries ->
          { name=country.Name; returns={kind="nested"; endpoint="/byCountry/pickTopic"}
            trace=[|"country=" + id |] } ])

    memberPath "/pickYear" (fun () ->
      [ for (KeyValue(id, year)) in worldBank.Years ->
          { name=year.Year; returns={kind="nested"; endpoint="/byYear/pickTopic"}
            trace=[|"year=" + id |] } ])

    memberPathf "/%s/pickTopic" (fun by ->
      [ for (KeyValue(id, top)) in worldBank.Topics ->
          { name=top.Name; returns={kind="nested"; endpoint="/" + by + "/pickIndicator/" + id}
            trace=[||] } ])

    memberPathf "/%s/pickIndicator/%s" (fun (by, topic) ->
      [ for ikey in worldBank.Topics.[topic].Indicators ->
          let ind = worldBank.Indicators.[ikey]
          let typ = 
              if by = "byYear" then { name="tuple"; ``params``=[| "string"; "float" |] }
              elif by = "byCountry" then { name="tuple"; ``params``=[| "int"; "float" |] }
              else failwith "bad request"
          let typ = { name="seq"; ``params``=[| typ |]}
          { name=ind.Name; returns={ kind="primitive"; ``type``= typ; endpoint="/data"}
            trace=[|"indicator=" + ikey |] } ])

    path "/data" >=> request (fun r ->
      let trace = 
        [ for kvps in (Utils.ASCII.toString r.rawForm).Split('&') ->
            match kvps.Split('=') with
            | [| k; v |] -> k, v
            | _ -> failwith "wrong trace" ] |> dict

      match trace with
      | (Lookup "year" y) & (Lookup "indicator" i) -> 
          let ydet, idet = worldBank.Years.[y], worldBank.Indicators.[i]
          worldBank.Data 
          |> Seq.filter (fun dt -> dt.Year = ydet.Index && dt.Indicator = idet.Index )
          |> Seq.map (fun dt -> worldBank.CountriesByIndex.[dt.Country].Name, dt.Value)
          |> formatPairSeq JsonValue.String
          |> Successful.OK
      | (Lookup "country" c) & (Lookup "indicator" i) -> 
          let cdet, idet = worldBank.Countries.[c], worldBank.Indicators.[i]
          worldBank.Data 
          |> Seq.filter (fun dt -> dt.Country = cdet.Index && dt.Indicator = idet.Index )
          |> Seq.map (fun dt -> worldBank.YearsByIndex.[dt.Year].Year, dt.Value)
          |> formatPairSeq JsonValue.String
          |> Successful.OK
      | _ -> failwith "wrong trace" ) ]