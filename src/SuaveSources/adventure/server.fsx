#I "../../../packages/samples"
#r "Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#r "Suave/lib/net40/Suave.dll"
open System
open System.IO
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

// ----------------------------------------------------------------------------
// Adventure provider
// ----------------------------------------------------------------------------

open Suave
open Suave.Filters
open Suave.Operators

type TypeNested = { kind:string; endpoint:string }
type Member = { name:string; returns:obj; trace:string[] }


type PageEntry = { Key: string; Text : string; Choices : (string * string) list }

let parseLine (line:string) = 
  let split = line.Split('|')
  let choices = split.[2].Split('¿')
  let niceKey (s:string) = s.ToLower().Replace(" ", "-").Trim()
  { Key = niceKey split.[0]
    Text = split.[1]
    Choices =
       if choices.Length <= 1 then []
       else [ for i in 0..2..choices.Length-1 -> (niceKey choices.[i], choices.[i+1]) ] }
        
let lookup = 
  File.ReadAllLines(__SOURCE_DIRECTORY__ + "/sample.dat")
  |> Seq.map parseLine
  |> Seq.map (fun p -> p.Key, p)
  |> dict

let app =
  choose [
    path "/" >=> 
      ( [| { name="Start the adventure..."; returns={kind="nested"; endpoint="/intro"}; trace=[||] } |]
        |> toJson |> Successful.OK )
    pathScan "/%s" (fun section ->
        [| for key, text in lookup.[section].Choices ->
            { name=text; returns={kind="nested"; endpoint="/"+key}; trace=[||] } |]
        |> toJson |> Successful.OK) ]
