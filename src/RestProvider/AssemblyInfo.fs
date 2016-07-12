namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("TheGamma.RestProvider")>]
[<assembly: AssemblyProductAttribute("RestProvider")>]
[<assembly: AssemblyDescriptionAttribute("F# type provider that lets you create type providers just by implementing a simple REST service")>]
[<assembly: AssemblyVersionAttribute("0.0.1")>]
[<assembly: AssemblyFileVersionAttribute("0.0.1")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.1"
    let [<Literal>] InformationalVersion = "0.0.1"
