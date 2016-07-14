(*** hide ***)
#nowarn "58"
#I "../../packages/samples"
#r "Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#r "Suave/lib/net40/Suave.dll"
open System
open System.IO
open System.Collections.Generic
open Newtonsoft.Json

open Suave
open Suave.Filters
open Suave.Operators

let serializer = JsonSerializer.Create()

let toJson value =
  let sb = System.Text.StringBuilder()
  use tw = new System.IO.StringWriter(sb)
  serializer.Serialize(tw, value)
  sb.ToString()

(*** define: contains ***)
let (|Contains|_|) k s = if Set.contains k s then Some() else None
(**
Minimal type provider sample
============================

This sample discusses the implementation of the server-side component of the minimal type
provider demo that is [discussed on the home page](index.html). You can also find the
complete source code for the server [in the GitHub
repository](https://github.com/fsprojects/RestProvider/blob/master/src/SuaveSources/minimal/server.fsx).
We're going to use lightweight F# library [Suave](https://suave.io/) to build the web server and
a helper function `toJson` that turns an F# record into JSON (also [in the full source
code](https://github.com/fsprojects/RestProvider/blob/8e20e01d4bae8d7efecb7013ca1d393cd3fa4d4a/src/SuaveSources/minimal/server.fsx#L15)).

REST provider protocol as F# records
------------------------------------

The first thing to do is to define records that model the relevant parts of the
[REST provider protocol](protocol.html). These directly map to JSON values, so writing
the code that returns the JSON data will be easy. The request to `/` and `/city` returns
information about provided type, which is just an array of members - we're going to represent
this as `Member[]` in F#. The type of a member can be either nested provided type, or a
primitive type. Those are repesented by `TypeNested` and `TypePrimitive`:
*)
/// Type of a member that is primitive type (int)
type TypePrimitive =
  { kind:string // This is always "primitive"
    ``type``:obj
    endpoint:string }

/// Type of a member that is another provided type
type TypeNested =
  { kind:string // This is always "nested"
    endpoint:string }

/// Represents a single member of a provided type
type Member =
  { name:string
    returns:obj // TypePrimitive or TypeNested
    trace:string[] }

(**
Implementing the service
------------------------

The service handles requests to `/` (the root type returning different cities), `/city`
(the "city" type with members for the various indicators) and `/data` (to actually load
the data based on a trace).

Using Suave, this can be easily expressed using the `choose` combinator that takes a list
of WebParts and runs the first one that can handle a request. We use `path "/city"` to
specify the required path. The actual code to handle the request is hidden in `(...)` and
we look at it next:

    let app =
      choose [
        path "/" >=> (*[omit:(...)]*)(...)(*[/omit]*)
        path "/city" >=> (*[omit:(...)]*)(...)(*[/omit]*)
        path "/data" >=> (*[omit:(...)]*)(...)(*[/omit]*) ]

*)
(**
### Providing root and city types

We are writing very minimal sample here, so the types representing the root (with city names as
members) and city (with indicators as members) do not load data from anywhere and instead return
just a constant F# value. Those directly correspond to the [examples on the homepage](index.html),
so we won't repeat everything here. The main point is that you could easily generate the records
and arrays from any data source that you wish:
*)
path "/" >=>
  ( [| { name="London"; trace=[|"London"|]
         returns={kind="nested"; endpoint="/city"} }
       { name="New York"; trace=[|"NYC"|]
         returns={kind="nested"; endpoint="/city"} } |]
    |> toJson |> Successful.OK )
(**
For the root type, we return members named `London` and `New York`. They are both of a nested type
provided by the `/city` endpoint (below) and they append an array of values that identify which
city we are accessing to the trace.

The implementation creates an array of `Member` types and passes it to the helper `toJson` function,
which serializes the data into JSON and then to `Successful.OK`, which creates a WebPart that
returns the resulting JSON as HTTP 200 response. The handler for `/city` is similar:
*)
path "/city" >=>
  ( [| { name="Population"; trace=[|"Population"|]
         returns={kind="primitive"; ``type``="int"; endpoint="/data"} }
       { name="Settled"; trace=[|"Settled"|]
         returns={kind="primitive"; ``type``="int"; endpoint="/data"} } |]
    |> toJson |> Successful.OK )
(**
The type of the `returns` field is `obj`, so we can set it to a value of the `TypeNested` record
(as we did in the first case) or the `TypePrimitive` record (as we did here). For primitive types,
we set `type="int"`, which tells the provider what should be the return type of the member
(the example here is very simple, but you can use more complex primitive types - see the
[protocol page for more info](protocol.html)).

### Returning data at runtime

When the provided code `cities.London.Population` is evaluated, the provider sends a `POST`
request to the `/data` endpoint. The body of the request includes trace values generated by the
individual members that were called along the way. In the above case, the request will include
body `London&Population`. The handler for the `/data` endpoint turns the body into a set and
uses pattern matching with a helper active pattern `Contains` to detect which of the values
should be returned:
*)
path "/data" >=> request (fun r ->
  match set (Utils.ASCII.toString(r.rawForm).Split('&')) with
  | Contains "London" & Contains "Population" -> Successful.OK "538689"
  | Contains "NYC" & Contains "Population" -> Successful.OK "550405"
  | Contains "London" & Contains "Settled" -> Successful.OK "-43"
  | Contains "NYC" & Contains "Settled" -> Successful.OK "1624"
  | _ -> RequestErrors.BAD_REQUEST "Wrong trace" )
(**
We are using the `request` function from Suave to get access to the `r.rawForm` value, which
contains the body of the request. The `Contains` active pattern that lets us nicely decide which
value to return is defined as follows:
*)
(*** include: contains ***)
(**
Summary
-------

In this tutorial, we looked at the simplest server that exposes data for the REST provider.
You can find the complete source code for the server [in the GitHub
repository](https://github.com/fsprojects/RestProvider/blob/master/src/SuaveSources/minimal/server.fsx)
and there are also couple of [other examples you can
explore](https://github.com/fsprojects/RestProvider/tree/master/src/SuaveSources). For more
information abotu the protocol, see a dedicated [REST provider protocol page](protocol.html).

*)
