#I "../../../packages/samples"
#r "Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#r "FSharp.Data/lib/net40/FSharp.Data.dll"
#r "Suave/lib/net40/Suave.dll"
open System
open System.IO
open System.Collections.Generic
open Newtonsoft.Json
open FSharp.Data

open Suave
open Suave.Filters
open Suave.Operators

// NOTE: This is currently limited to interactive scenarios, because
// it assumes that the code is typechecked before data is accessed
// - we need to store the original URL in the 'pivot-*' URLs so that
// we can always recover the original field info.
//
// Or perhaps we should use TRACE! 



// ----------------------------------------------------------------------------
// Server
// ----------------------------------------------------------------------------

type MemberQuery = JsonProvider<"""
  [ { "name":"United Kingdom", "returns":{"kind":"nested", "endpoint":"/country"}, "trace":["country", "UK"],
      "documentation":"can be a plain string" }, 
    { "name":"United Kingdom", "returns":{"kind":"nested", "endpoint":"/country"},
      "documentation":{"endpoint":"/or-a-url-to-call"} }, 
    { "name":"Population", "returns":{"kind":"primitive", "type":null, "endpoint":"/data"}, "trace":["indicator", "POP"] } ] """>

type TypeInfo = JsonProvider<"""
  [ "float",
    { "name":"record", "fields":[ {"name":"A","type":null} ] },
    { "name":"seq", "params":[ null ] } ]
  """, SampleIsList=true>

let (|ComplexType|_|) (js:JsonValue) =
  TypeInfo.Root(js).Record
  |> Option.map (fun r -> r.Name, [ for p in r.Params -> p.JsonValue ], r.Fields)

let (|RecordSeqMember|_|) (membr:MemberQuery.Root) =
  if membr.Returns.Kind = "primitive" then
    match membr.Returns.Type.JsonValue with
    | ComplexType("seq", [ComplexType("record", _, fields)], _) -> Some(fields)
    | _ -> None
  else None

type StoredFields = 
  { Fields : TypeInfo.Field list
    Endpoint : string list }

type Message = 
  | StoreFields of string * StoredFields * AsyncReplyChannel<string>
  | LookupId of string * AsyncReplyChannel<StoredFields option>

let agent = MailboxProcessor.Start (fun inbox ->
  let rec loop memberToId idToFields = async {
    let! msg = inbox.Receive()
    match msg with
    | LookupId(id, repl) ->
        repl.Reply(Map.tryFind id idToFields)
        return! loop memberToId idToFields
    | StoreFields(membr, fields, repl) ->
        match Map.tryFind membr memberToId with
        | Some id -> 
            repl.Reply(id)
            return! loop memberToId idToFields
        | None ->
            let id = sprintf "inject-%d" (memberToId |> Seq.length)
            repl.Reply(id)
            return! loop (Map.add membr id memberToId) (Map.add id fields idToFields) }
  loop Map.empty Map.empty )

let storeFields membr fields = agent.PostAndReply(fun ch -> StoreFields(membr, fields, ch))

let (|Injected|_|) id = 
  agent.PostAndReply(fun ch -> LookupId(id, ch))

let makeRecordSeq fields = 
  let recd = TypeInfo.Record("record", Array.ofSeq fields, [||])
  TypeInfo.Record("seq", [||], [| recd.JsonValue |]).JsonValue
  
let originalRequest parts ctx = 
  let another = "http://localhost:10042/" + String.concat "/" parts
  let body = 
    if ctx.request.rawForm.Length = 0 then None
    else Some(HttpRequestBody.BinaryUpload ctx.request.rawForm)
  Http.AsyncRequestString
    ( another, [], httpMethod = ctx.request.``method``.ToString(), ?body = body )

//   unique/drop1/drop2
let app ctx = async {
  let local = ctx.request.url.LocalPath.Split([|'/'|], StringSplitOptions.RemoveEmptyEntries) |> List.ofSeq
  match local with
  | [] -> return! RequestErrors.NOT_FOUND "Source server prefix missing." ctx
  | another::"pivot-data"::((Injected fields) as injectid)::rest ->
      let droppedFields = set rest
      let leftFields = fields.Fields |> Seq.mapi (fun i v -> 
        if droppedFields.Contains(sprintf "field-%d" i) then None
        else Some v.Name) |> Seq.choose id |> set

      let! data = originalRequest (another::fields.Endpoint) ctx
      match JsonValue.Parse(data) with
      | JsonValue.Array(objs) ->
          let newObjs = objs |> Array.map (function
            | JsonValue.Record(flds) -> 
                flds |> Array.filter (fst >> leftFields.Contains) |> JsonValue.Record
            | j -> j)
          return! Successful.OK (JsonValue.Array(newObjs).ToString()) ctx
      | _ -> return! ServerErrors.INTERNAL_ERROR "Data was not a JSON array" ctx

  | another::"pivot-nested"::((Injected fields) as injectid)::rest ->
      let droppedFields = set rest
      let fields = fields.Fields |> Seq.mapi (fun i v -> sprintf "field-%d" i, v)

      let dataRet = 
        let dataRetTyp = 
          fields |> Seq.filter (fst >> droppedFields.Contains >> not) |> Seq.map snd |> makeRecordSeq
        let endpoint = "/pivot-data/" + injectid + "/" + String.concat "/" rest
        MemberQuery.Returns("primitive", endpoint, dataRetTyp)
      
      printfn "%A" dataRet

      let members =
        [| yield MemberQuery.Root("data", dataRet, [||], MemberQuery.StringOrDocumentation("Return the data")).JsonValue
           for id, field in fields do
              if not (droppedFields.Contains id) then
                let endpoint = "/pivot-nested/" + injectid + "/" + String.concat "/" (id::rest)
                let ret = MemberQuery.Returns("nested", endpoint, JsonValue.Null)                      
                let doc = sprintf "Removes the field '%s' from the returned data set" field.Name
                yield MemberQuery.Root("drop " + field.Name, ret, [||], MemberQuery.StringOrDocumentation("")).JsonValue |]
      return! Successful.OK (JsonValue.Array(members).ToString()) ctx

  | another::local -> 
      // Request the data from the original source
      let! data = originalRequest (another::local) ctx 
      
      // If it is a POST request, inject 
      let transformed = 
        if ctx.request.``method`` = HttpMethod.GET then
          let members = 
            MemberQuery.Parse(data)
            |> Array.map (fun membr ->
                match membr with
                | RecordSeqMember fields ->
                    let id = 
                      storeFields 
                        (ctx.request.url.ToString() + "\n" + membr.Name) 
                        { Fields = List.ofSeq fields; 
                          Endpoint = membr.Returns.Endpoint.Split([|'/'|], StringSplitOptions.RemoveEmptyEntries) |> List.ofSeq }
                    let ret = MemberQuery.Returns("nested", "/pivot-nested/" + id, JsonValue.Null)
                    MemberQuery.Root(membr.Name, ret, membr.Trace, membr.Documentation).JsonValue
                | _ -> membr.JsonValue)
            |> JsonValue.Array
          members.ToString()
        else data
              
      return! Successful.OK transformed ctx }
