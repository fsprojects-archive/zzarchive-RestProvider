namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("RestProvider")>]
[<assembly: AssemblyProductAttribute("RestProvider")>]
[<assembly: AssemblyDescriptionAttribute("F# type provider that lets you create type providers just by implementing a simple REST service")>]
[<assembly: AssemblyVersionAttribute("1.0")>]
[<assembly: AssemblyFileVersionAttribute("1.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0"
    let [<Literal>] InformationalVersion = "1.0"
