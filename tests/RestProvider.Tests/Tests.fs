#if INTERACTIVE
#r "../../bin/TheGamma.RestProvider.dll"
#r "../../packages/test/NUnit/lib/nunit.framework.dll"
#else
module RestProvider.Tests
#endif
open TheGamma
open NUnit.Framework

type Minimal = RestProvider<"http://localhost:10042/minimal">

[<Test>]
let ``Minimal sample returns population of London`` () =
  let population = Minimal.London.Population
  Assert.AreEqual(538689, population)
