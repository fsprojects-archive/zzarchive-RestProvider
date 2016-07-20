(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use
// it to define helpers that you do not want to show in the documentation.
#I "../../bin"
(**
Pivot sample
======================================
*)
#r "TheGamma.RestProvider.dll"
open TheGamma

type O = 
  RestProvider<
    "http://localhost:10042/pivot", 100, 
    "source=http://localhost:10042/olympics">

let r = 
  O.data
      .``group data``.``by Athlete``
        .``count all``.``sum Gold``.``sum Silver``.``sum Bronze``
        .``concatenate values of NOC``.``then``
      .``sort data``
        .``by Gold descending``
        .``and by Silver descending``
        .``and by Bronze descending``.``then``
      .paging.skip(1).take(10).``get the data``

for r in r do printfn "%A" r
