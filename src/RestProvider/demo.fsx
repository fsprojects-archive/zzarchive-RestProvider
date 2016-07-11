#I "bin/Debug"
#r "GammaProvider.dll"
#r "FSharp.Data.dll"
open TheGamma

type A = GammaProvider<"http://127.0.0.1:10050">
A.