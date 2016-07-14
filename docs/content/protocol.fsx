(**

REST Provider protocol
======================

This page provides some details on the format of the responses that the REST type provider
understands. This is still work in progress, so expect that things will change in the future.
Also, please share your thoughts [as GitHub issues](https://github.com/fsprojects/RestProvider/issues)!

Provided type endpoint
----------------------

When the REST provider calls your root URL or an endpoint specifying a nested type, the resulting
`<type>` should be an array of members. Those will be turned into members of the provided type:
*)
(*** hide,define-output:it1 ***)
"""JsonSpec:
<type> =
  [ <member>, ... ]

<member> =
  { "name" : <string>,
    "returns" : <returns>,
    "trace"? : [<string>, ... ],
    "documentation"? : <documentation> }"""
(*** include-it:it1 ***)
(**
Each member needs to have a `name` and `returns`, which is the type of the result. It can
optionally have `trace`, which specifies an array of strings that will be passed to the endpoint
that returns data (once we reach a primitive type). A member can also optionally have a
documentation (displayed in a tooltip):
*)
(*** hide,define-output:it2 ***)
"""JsonSpec:
<documentation> =
  | <string>
  | { "endpoint": <string> }

<returns> =
  | { "kind": "nested", "endpoint": <string> }
  | { "kind": "primitive", "endpoint": <string>, "type":<type> }"""
(*** include-it:it2 ***)
(**
A documentation is either an inline string or a record with a URL that should be called to
obtain the string. The return type can be one of two things:

 - **Nested type** means that the type of the member is another provided type specified by
   an endpoint that returns another `<type>` specification.
 - **Primitive type** means that the result is a primitive type. In this case, the provider
   generates code to call the specified `endpoint` to get the data and parse it according to
   the specified `type`.

The type can be one of a few primitive types (`string`, `float` and `int`) or one of more complex
types, including sequence (collection), record (with named fields) or a tuple (with two unnamed
fields):
*)
(*** hide,define-output:it3 ***)
"""JsonSpec:
<type> =
  | "string"
  | "float"
  | "int"
  | { "name":"record", "fields":[<field>, ...] }
  | { "name":"seq", "params":[<type>] }
  | { "name":"tuple", "params":[<type>, <type>] }

<field> =
  { "name":<string>, "type":<type> }"""
(*** include-it:it3 ***)
(**
The following section discusses the format of the data that should be returned by the `endpoint`
for accessing data.

Data endpoint
-------------

When you evaluate some code that returns primitive data such as `cities.London.Population`,
the REST provider makes a `POST` request to the given endpoint with a body set to the `trace`
values collected along the way and concatenated using `&` (as demonstrated in the [home page
example](index.html)).

The result should be formatted as follows:

 - For `string`, `float` or `int`, the result should simply return plain text with the
   value (plain-text string, integer such as `42` or a floating point number such as `3.1415`).

 - For `seq`, the result should be a JSON array `[ ... ]` containing individual values of the
   sequence (recursively formatted using the rules given here).

 - For `tuple`, the result should be a JSON array containing two values `[ v1, v2 ]`,
   formatted recursively using the rules given here.

 - For `record`, the result should be a JSON object with fields corresponding to the
   fields of the record type. The values for the fields should be, again, formatted recursively.

To give a concrete example, say we have a sequence of records with a single field. The type
returned by the type endpoint would specify this as follows:
*)
(*** hide,define-output:it4 ***)
"""JsonSpec:
{ "name":"seq",
  "params": [
    { "name":"record", "fields":[
      { "name":"demo", "type":"int" } ] }
] }"""
(*** include-it:it4 ***)
(**
A valid value returned by the data endpoint for the above type is a JSON array of objects
with the `demo` field:

    [lang=javascript]
    [ { "demo":1 },
      { "demo":2 },
      { "demo":42 } ]


*)
