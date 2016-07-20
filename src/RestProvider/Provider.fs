namespace RestProvider

open ProviderImplementation.ProvidedTypes
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Reflection
open System
open System.Reflection
open FSharp.Data
open System.Threading.Tasks
open ProviderImplementation.QuotationBuilder

type MemberQuery = JsonProvider<"""
  [ { "name":"United Kingdom", "returns":{"kind":"nested", "endpoint":"/country"}, "trace":["country", "UK"],
      "parameters":[ {"name":"count", "type":null}, {"name":"another", "type":null} ],
      "documentation":"can be a plain string" }, 
    { "name":"United Kingdom", "returns":{"kind":"nested", "endpoint":"/country"},
      "documentation":{"endpoint":"/or-a-url-to-call"} }, 
    { "name":"Population", "returns":{"kind":"primitive", "type":null, "endpoint":"/data"}, "trace":["indicator", "POP"] } ] """>

type TypeInfo = JsonProvider<"""
  [ "float",
    { "name":"record", "fields":[ {"name":"A","type":null} ] },
    { "name":"seq", "params":[ null ] } ]
  """, SampleIsList=true>

module Helpers = 
  type Message =
    | SetTimeout of int
    | Download of string * string * AsyncReplyChannel<Choice<string, exn>>

  let parseCookieString (s:string) = 
    s.Split([|'&'|], StringSplitOptions.RemoveEmptyEntries) |> Array.choose (fun s -> 
      match s.Split('=') |> List.ofSeq with 
      | [] -> None
      | [ s ] -> Some (s, "")
      | k::v::_ -> Some(k, v) )

  let agent = MailboxProcessor.Start(fun inbox -> async {
    let cache = Collections.Generic.Dictionary<string * string, DateTime * string>()
    let mutable timeout = 3600
    while true do
      let! msg = inbox.Receive()
      match msg with 
      | SetTimeout t -> timeout <- t
      | Download(url, cookies, repl) ->
        try 
          match cache.TryGetValue( (url, cookies) ) with
          | true, (dt, res) when (DateTime.Now - dt).TotalSeconds < float timeout -> 
              repl.Reply(Choice1Of2(res))
          | _ ->
            let! res = Http.AsyncRequestString(url, cookies=parseCookieString cookies)
            cache.[(url, cookies)] <- (DateTime.Now, res)
            repl.Reply(Choice1Of2 res)
        with e ->
          repl.Reply(Choice2Of2 e) })
  
  let setTimeout t = agent.Post(SetTimeout t)

  let cachedMemberQuery url cookies = async {
    let! res = agent.PostAndAsyncReply(fun r -> Download(url, cookies, r))
    match res with
    | Choice1Of2 body -> return MemberQuery.Parse(body)
    | Choice2Of2 e -> return raise e }

type RuntimeValue =
  | Json of JsonValue 
  | String of string
  member x.AsJson() = 
    match x with Json j -> j | String s -> JsonValue.Parse s

type RuntimeContext(root:string, cookies:string, trace:string) = 
  member x.Root = root
  member x.Trace = trace
  member x.AddTraces(names:string[], suffixes:obj[]) = 
    Array.zip names suffixes
    |> Seq.fold (fun (x:RuntimeContext) (n, v) -> x.AddTrace(n + "=" + v.ToString())) x
  member x.AddTrace(suffix) = 
    let traces = 
      [ if not (String.IsNullOrEmpty trace) then yield trace
        if not (String.IsNullOrEmpty suffix) then yield suffix ]
    RuntimeContext(root, cookies, String.concat "&" traces)
  member x.GetValue(endpoint:string) =     
    Http.RequestString
      ( root.TrimEnd('/') + "/" + endpoint.TrimStart('/'), httpMethod="POST", body=TextRequest trace, 
        cookies=Helpers.parseCookieString cookies )
    |> String

module Parsers = 
  let (|AsInt32|_|) s = match Int32.TryParse(s) with true, r -> Some r | _ -> None
  let (|AsFloat|_|) s = match Double.TryParse(s) with true, r -> Some r | _ -> None

type Runtime = 
  static member SplitSequence<'T>(input:RuntimeValue, f:RuntimeValue -> 'T) =
    match input.AsJson() with
    | JsonValue.Array els -> els |> Seq.map (Json >> f)
    | _ -> failwith "SplitSequence: Expected array"
  static member SplitTuple<'T1, 'T2>(input:RuntimeValue, f:RuntimeValue -> 'T1, g:RuntimeValue -> 'T2) =
    match input.AsJson() with
    | JsonValue.Array [| e1; e2 |] -> f (Json e1), g (Json e2)
    | _ -> failwith "SplitTuple: Expected two-element tuple"    
  static member ParseFloat(input:RuntimeValue) = 
    match input with
    | String s -> float s
    | Json(JsonValue.String (Parsers.AsFloat f))
    | Json(JsonValue.Float f) -> f
    | Json(JsonValue.Number n) -> float n
    | _ -> failwith "ParseFloat: Expected number or float"
  static member ParseInt(input:RuntimeValue) = 
    match input with
    | String s -> int s
    | Json(JsonValue.String(Parsers.AsInt32 n)) -> n
    | Json(JsonValue.Float f) -> int f
    | Json(JsonValue.Number n) -> int n
    | _ -> failwith "ParseInt: Expected number"
  static member ParseString(input:RuntimeValue) = 
    match input with
    | String s -> s
    | Json(JsonValue.String s) -> s
    | Json(JsonValue.Null) -> null
    | Json(JsonValue.Float f) -> string f
    | Json(JsonValue.Number n) -> string n
    | _ -> failwith "ParseString: Expected string"
  static member GetRecordField<'T>(input:RuntimeValue, name, f) : 'T =
    match input.AsJson() with
    | JsonValue.Record(fields) -> 
        fields |> Seq.pick (fun (fld, v) -> if fld = name then Some(f (Json v)) else None)
    | _ -> failwith "Not record"

module NiceNames = 
  let namesSet = ref (set [""; null])
  let niceName (name) = 
    let names = seq { 
      yield name
      for i in 0 .. Int32.MaxValue do 
        yield sprintf "%s%d" name i }
    let firstName = names |> Seq.find (fun name -> not (namesSet.Value.Contains name))
    namesSet := namesSet.Value.Add(firstName)
    firstName

type ResultType = 
  | Primitive of string
  | Generic of string * ResultType list
  | Record of (string * ResultType) list

type GenerationContext  =
  { DomainType : ProvidedTypeDefinition 
    Records : System.Collections.Concurrent.ConcurrentDictionary<ResultType, System.Type * System.Type * (Expr -> Expr)> }

[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module ResultType = 
  let rec fromJson (json:JsonValue) =
    let sot = TypeInfo.Root(json)
    match sot.Record, sot.String with
    | Some typ, _ when typ.Name = "record" -> Record [ for t in typ.Fields -> t.Name, fromJson t.Type.JsonValue ]
    | Some typ, _ -> Generic(typ.Name, [ for t in typ.Params -> fromJson t.JsonValue ] )
    | _, Some prim -> Primitive prim
    | _ -> failwith "invalid type spec"

  let runtime = typeof<Runtime>

  let rec getTypeAndParser ctx typ : Type * Type * (Expr -> Expr) = 
    match typ with
    | Primitive "string" -> 
        typeof<string>, 
        typeof<string>, 
        fun e -> runtime?ParseString () (e)

    | Primitive "float" -> 
        typeof<float>, 
        typeof<float>, 
        fun e -> runtime?ParseFloat () (e)

    | Primitive "int" -> 
        typeof<int>, 
        typeof<int>, 
        fun e -> runtime?ParseInt () (e)

    | Record fields ->
        ctx.Records.GetOrAdd(typ, fun _ -> // ResultType.Primitive "zz"
          let record = ProvidedTypeDefinition(NiceNames.niceName "record", None)
          //let record = typeof<obj>
          ctx.DomainType.AddMember(record) 
          for name, typ in fields do
            let fieldTyp, erasedTyp, parser = getTypeAndParser ctx typ
            let p = ProvidedProperty(name, fieldTyp)
            p.GetterCode <- fun [self] -> runtime?GetRecordField (fieldTyp) (self, name, makeFunction parser)
            record.AddMember(p)
          record :> System.Type, typeof<obj>, fun e -> e)
         
    | Generic("tuple", [typ1; typ2]) ->
        let nestedTyp1, erasedTyp1, nestedParser1 = getTypeAndParser ctx typ1
        let nestedTyp2, erasedTyp2, nestedParser2 = getTypeAndParser ctx typ2
        FSharpType.MakeTupleType [| nestedTyp1; nestedTyp2 |],
        FSharpType.MakeTupleType [| erasedTyp1; erasedTyp2 |],
        fun e -> runtime?SplitTuple (erasedTyp1, erasedTyp2) (e, makeFunction nestedParser1, makeFunction nestedParser2)

    | Generic("seq", [typ]) -> //getTypeAndParser ctx typ       
        let nestedTyp, erasedTyp, nestedParser = getTypeAndParser ctx typ       
        typedefof<seq<_>>.MakeGenericType [| nestedTyp |],
        typedefof<seq<_>>.MakeGenericType [| erasedTyp |],
        fun e -> runtime?SplitSequence (erasedTyp) (e, makeFunction nestedParser)

    | t -> failwith (sprintf "Unsupported type: %A" t)

[<TypeProvider>]
type public RestProvider() as this = 
  inherit TypeProviderForNamespaces()

  let thisAssembly = System.Reflection.Assembly.GetExecutingAssembly()
  let rootNamespace = "TheGamma" 

  let knownTypes = System.Collections.Concurrent.ConcurrentDictionary<string, System.Type>()
  let runtimeContext = typeof<RuntimeContext>
  let records = System.Collections.Concurrent.ConcurrentDictionary<ResultType, System.Type * System.Type * (Expr -> Expr)>()
  
  let invalidator = MailboxProcessor.Start(fun inbox -> async {
    let mutable lastRefresh = DateTime.Now
    while true do 
      let! timeout = inbox.Receive()
      if (DateTime.Now - lastRefresh).TotalSeconds > float timeout then
        this.Invalidate()
        lastRefresh <- DateTime.Now })

  let checkMethodParameters (pars:MemberQuery.Parameter[]) = 
    pars |> Array.map (fun p ->
      match ResultType.fromJson p.Type.JsonValue with
      | Primitive "int" -> ProvidedParameter(p.Name, typeof<int>)
      | Primitive "string" -> ProvidedParameter(p.Name, typeof<string>)
      | _ -> failwith "Only int or string parameters are supported." )

  let provideType typeName (source:string) timeout cookies = 
    invalidator.Post(timeout)
    Helpers.setTimeout (timeout / 2)
    
    let root = ProvidedTypeDefinition(thisAssembly, rootNamespace, typeName, baseType=Some typeof<obj>, HideObjectMethods=true)
    let types = ProvidedTypeDefinition("types", None)
    root.AddMember(types)

    //let record = ProvidedTypeDefinition("record1234", Some typeof<obj>)
    //types.AddMember(record)
    //records.GetOrAdd(ResultType.Primitive "zz", fun _ -> record :> System.Type, id) |> ignore
    //records.GetOrAdd(ResultType.Primitive "zz", fun _ -> typeof<int>, id) |> ignore

    let rec provideMembers source statc (members:Task<MemberQuery.Root[]>) =
      [ for membr in members.Result ->
          let trace = membr.Trace |> String.concat "&"
          let typ, altdoc, getter = 
            match membr.Returns.Kind with
            | "nested" -> 
                provideType source membr.Returns.Endpoint, 
                membr.Returns.Endpoint,
                fun ctx -> ctx :> Microsoft.FSharp.Quotations.Expr
            | "primitive" -> 
                let endpoint = membr.Returns.Endpoint
                let typ, erasedTyp, parser = ResultType.getTypeAndParser { DomainType = types; Records = records } (ResultType.fromJson membr.Returns.Type.JsonValue)
                typ, "primitive", fun ctx -> parser (runtimeContext?GetValue () (ctx, endpoint))
            | t -> failwithf "Unknown type: %s" t
          
          let mi, addDoc, addDocDelay = 
            if membr.Parameters.Length = 0 then
              let p = 
                ProvidedProperty
                  ( membr.Name, typ, IsStatic = statc,
                    GetterCode = (fun args -> 
                      if statc then getter <@ RuntimeContext(source, cookies, "").AddTrace(trace) @>
                      else getter <@ (unbox<RuntimeContext> %%(List.head args)).AddTrace(trace) @> ))
              p :> MemberInfo, p.AddXmlDoc, p.AddXmlDocComputed
            else 
              let pars = checkMethodParameters membr.Parameters |> List.ofSeq
              let m = 
                ProvidedMethod
                  ( membr.Name, pars, typ, IsStaticMethod = statc, 
                    InvokeCode = (fun args ->      
                      let methArgs = if statc then args else List.tail args
                      let methArgsObjs = methArgs |> List.map (fun e -> Expr.Coerce(e, typeof<obj>))
                      let methArgsExpr = Expr.NewArray(typeof<obj>, methArgsObjs)
                      let parNamesExpr = Expr.NewArray(typeof<string>, [ for p in pars -> Expr.Value(p.Name) ])
                      if statc then 
                        getter <@ RuntimeContext(source, cookies, "").AddTrace(trace).AddTraces(%%parNamesExpr, %%methArgsExpr) @>
                      else getter <@ (unbox<RuntimeContext> %%(List.head args)).AddTrace(trace).AddTraces(%%parNamesExpr, %%methArgsExpr) @> ))
              m :> MemberInfo, m.AddXmlDoc, m.AddXmlDocComputed

          match membr.Documentation.Record, membr.Documentation.String with
          | Some recd, _ -> 
              let doc = Http.AsyncRequestString(recd.Endpoint) |> Async.StartAsTask
              addDocDelay(fun _ -> "<summary>" + doc.Result + "</summary>")
          | _, Some str -> addDoc ("<summary>" + str + "</summary>")
          | _ -> addDoc ("<summary>/" + altdoc + "</summary>")
          mi ]

    and provideType (source:string) (endpoint:string) =
      let url = source.TrimEnd('/') + "/" + endpoint.TrimStart('/')
      knownTypes.GetOrAdd(url, fun url ->
        let name = endpoint.Split('/') |> Seq.last
        let ty = ProvidedTypeDefinition(NiceNames.niceName name, Some typeof<obj>, HideObjectMethods=true)
        let members = Helpers.cachedMemberQuery url cookies |> Async.StartAsTask
        types.AddMember(ty)
        ty.AddMembersDelayed(fun () -> provideMembers source false members)
        ty :> System.Type )

    let members = async {
      let! data = Http.AsyncRequestString(source, cookies=Helpers.parseCookieString cookies)
      return MemberQuery.Parse(data) } |> Async.StartAsTask

    root.AddMembersDelayed (fun () -> provideMembers source true members)
    root



  let gammaType = 
    ProvidedTypeDefinition
      (thisAssembly, rootNamespace, "RestProvider", Some(typeof<obj>), HideObjectMethods = true)
  let staticParams = 
    [ ProvidedStaticParameter("Source", typeof<string>, "")
      ProvidedStaticParameter("Timeout", typeof<int>, 3600)
      ProvidedStaticParameter("Cookies", typeof<string>, "") ]

  
  let helpText = """
    <summary>A provider</summary>
    <param name="Source">A thing</param>"""

  do gammaType.AddXmlDoc(helpText)
  do gammaType.DefineStaticParameters(staticParams, fun typeName providerArgs -> 
        let source = (providerArgs.[0] :?> string)
        let timeout = (providerArgs.[1] :?> int)
        let cookies = (providerArgs.[2] :?> string)
        provideType typeName source timeout cookies)
  do base.AddNamespace(rootNamespace, [ gammaType ])

[<TypeProviderAssembly>]
do ()