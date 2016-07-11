#r "../packages/Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#r "../packages/Suave/lib/net40/Suave.dll"
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
// Server
// ----------------------------------------------------------------------------

open Suave
open Suave.Filters
open Suave.Operators

type GenericType = { name:string; ``params``:obj[] }
type TypePrimitive = { kind:string; ``type``:obj; endpoint:string }
type TypeNested = { kind:string; endpoint:string }
type Member = { name:string; returns:obj; trace:string[] }

let (|Contains|_|) k s = if Set.contains k s then Some() else None

let app =
  choose [
    path "/" >=> 
      ( [| { name="London"; returns={kind="nested"; endpoint="/city"}; trace=[|"London"|] };
           { name="New York"; returns={kind="nested"; endpoint="/city"}; trace=[|"NYC"|] } |]
        |> toJson |> Successful.OK )

    path "/city" >=>
      ( [| { name="Population"; returns={kind="primitive"; ``type``="int"; endpoint="/data"}; trace=[|"Population"|] };
           { name="Settled"; returns={kind="primitive"; ``type``="int"; endpoint="/data"}; trace=[|"Settled"|] } |]
        |> toJson |> Successful.OK )

    path "/data" >=> request (fun r ->
      match set (Utils.ASCII.toString(r.rawForm).Split('&')) with
      | Contains "London" & Contains "Population" -> Successful.OK "538689"
      | Contains "NYC" & Contains "Population" -> Successful.OK "550405"
      | Contains "London" & Contains "Settled" -> Successful.OK "-43"
      | Contains "NYC" & Contains "Settled" -> Successful.OK "1624"
      | _ -> RequestErrors.BAD_REQUEST "Wrong trace" ) ]

let config = 
  { defaultConfig with 
      logger = Logging.Loggers.saneDefaultsFor Logging.LogLevel.Verbose
      bindings = [ HttpBinding.mkSimple HTTP "127.0.0.1" 10047 ] }

let _, run = startWebServerAsync config app
let cts = new System.Threading.CancellationTokenSource()
Async.Start(run, cts.Token)
cts.Cancel()
