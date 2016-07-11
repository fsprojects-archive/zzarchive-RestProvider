#I "../../../packages/samples"
#r "System.Xml.Linq.dll"
#r "FSharp.Data/lib/net40/FSharp.Data.dll"
#r "Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#load "domain.fs"
open FSharp.Data
open System.IO
open System.Collections.Generic
open Newtonsoft.Json
open WorldBank.Domain

// ------------------------------------------------------------------------------------------------
// Download WorldBank meta-data from the internet
// ------------------------------------------------------------------------------------------------

type Indicators = JsonProvider<"http://api.worldbank.org/indicator?per_page=120&format=json">
type Countries = JsonProvider<"http://api.worldbank.org/country?per_page=100&format=json">

let getIndicators() = async {
  let! first = Http.AsyncRequestString("http://api.worldbank.org/indicator?per_page=100&format=json")
  let first = Indicators.Parse(first)
  let! records = 
    [ for p in 1 .. first.Record.Pages -> async {
        let url = "http://api.worldbank.org/indicator?per_page=100&format=json&page=" + (string p)
        let! data = Http.AsyncRequestString url
        printfn "Downloaded page: %d/%d" p first.Record.Pages
        let page = Indicators.Parse(data)
        return page.Array } ] |> Async.Parallel
  return Array.concat records }

let getCountries region = async {
  let region = match region with None -> "" | Some region -> "&region=" + region
  let! first = Http.AsyncRequestString("http://api.worldbank.org/country?per_page=100&format=json" + region) 
  let first = Countries.Parse(first)          
  let! records = 
    [ for p in 1 .. first.Record.Pages -> async {
        let url = "http://api.worldbank.org/country?per_page=100&format=json&page=" + (string p) + region
        let! data = Http.AsyncRequestString url
        printfn "Downloaded page: %d/%d" p first.Record.Pages
        let page = Countries.Parse(data)
        return page.Array } ] |> Async.Parallel
  return Array.concat records }

/// Meta-data about countries
let countriesMeta =
  let data = getCountries None |> Async.RunSynchronously
  data |> Array.map (fun c ->
    c.Iso2Code.ToUpper(), 
    { Index = 0us; DataPoints = -1; ID = c.Id.ToUpper(); Iso2Code = c.Iso2Code
      Name = c.Name; Capital = defaultArg c.CapitalCity ""; RegionId = c.Region.Id
      RegionName = c.Region.Value; IncomeLevel = c.IncomeLevel.Value } ) |> dict

/// Meta-data about indicators
let indicatorsMeta =  
  let data = getIndicators() |> Async.RunSynchronously
  data |> Array.map (fun i ->
    i.Id.ToUpper(),
    { Index = 0us; DataPoints = -1; ID = i.Id; Name = i.Name; Source = i.Source.Value; 
      SourceNote = i.SourceNote; SourceOrg = i.SourceOrganization; 
      Topics = List.choose id [ for t in i.Topics -> t.Value ] }) |> dict

// ------------------------------------------------------------------------------------------------
// Read the raw data from RDF files on disk 
// ------------------------------------------------------------------------------------------------

// Folder with World Bank RDF data
let [<Literal>] root = __SOURCE_DIRECTORY__ + "/data"
let [<Literal>] sample = root + "/GB.TAX.INTT.CN.rdf"
type Rdf = XmlProvider<sample>

// Lookup tables mpping keys to indices
let countriesLookup = Dictionary<string, uint16>()
let indicatorsLookup = Dictionary<string, uint16>()
let yearLookup = Dictionary<string, uint16>()
let data = ResizeArray<DataPoint>(10000000)

/// Helper that builds a function for writing data
let mapKey (d:Dictionary<string, _>) =
  let counter = ref 0us
  fun (k:string) ->
    let k = k.Substring(k.LastIndexOf('/')+1).ToUpper()
    match d.TryGetValue(k) with
    | true, n -> n
    | false, _ ->
        counter.Value <- counter.Value + 1us
        d.[k] <- counter.Value
        counter.Value

let mapArea = mapKey countriesLookup
let mapIndicator = mapKey indicatorsLookup
let mapYear = mapKey yearLookup

// Populates the 'data' array and lookup dictionaries
let mutable processed = 0
for f in Directory.GetFiles(root, "*.rdf") do
  try 
    processed <- processed + 1
    if processed % 50 = 0 then printfn "Processed: %d" processed
    for d in Rdf.Load(f).Descriptions do
      DataPoint
        ( mapArea d.RefArea.Resource,
          mapIndicator d.Indicator.Resource,
          mapYear d.RefPeriod.Resource,
          float d.ObsValue.XElement.Value ) |> data.Add
  with e ->   
    printfn "Failed for %s" f

// ----------------------------------------------------------------------------
// Build the final 'Data' value from everything we loaded
// ----------------------------------------------------------------------------

// Intersect the keys to get known countries & indicators
let knownCountries = 
  Set.intersect (set countriesMeta.Keys) (set countriesLookup.Keys)
let knownIndicators = 
  Set.intersect (set indicatorsMeta.Keys) (set indicatorsLookup.Keys)

// Calculate counts of data points
let countsByCountry = data |> Seq.countBy (fun d -> d.Country) |> dict
let countsByIndicator = data |> Seq.countBy (fun d -> d.Indicator) |> dict
let countsByYears = data |> Seq.countBy (fun d -> d.Year) |> dict

// Aggregated data set with all World Bank data
let worldBank = 
  { Countries = 
      [ for c in knownCountries ->
          c, { countriesMeta.[c] with
                 DataPoints = countsByCountry.[countriesLookup.[c]]
                 Index = countriesLookup.[c] } ] |> dict
    Indicators = 
      [ for c in knownIndicators ->
          c, { indicatorsMeta.[c] with
                 DataPoints = countsByIndicator.[indicatorsLookup.[c]]
                 Index = indicatorsLookup.[c] } ] |> dict    
    Years = 
      [ for y in yearLookup.Keys ->
          y, { Year = y
               Index = yearLookup.[y]
               DataPoints = countsByYears.[yearLookup.[y]] } ] |> dict    
    Data = data
    // Filled when loading data 
    Topics = null 
    CountriesByIndex = null
    IndicatorsByIndex = null
    YearsByIndex = null }


// ----------------------------------------------------------------------------
// Serialize 'worldBank' data
// ----------------------------------------------------------------------------

// Folder with pre-processed & saved data
let [<Literal>] cache = __SOURCE_DIRECTORY__ + "/cache"

let serializer = JsonSerializer.Create()

let toJson value = 
  let sb = System.Text.StringBuilder()
  use tw = new System.IO.StringWriter(sb)
  serializer.Serialize(tw, value)
  sb.ToString() 

// Save the metadata into three separate files
let countriesJson = toJson [| for (KeyValue(k, v)) in worldBank.Countries -> k, v |]
File.WriteAllText(cache + "/countries.json", countriesJson)

let indicatorsJson = toJson [| for (KeyValue(k, v)) in worldBank.Indicators -> k, v |]
File.WriteAllText(cache + "/indicators.json", indicatorsJson)

let yearsJson = toJson [| for (KeyValue(k, v)) in worldBank.Years -> k, v |]
File.WriteAllText(cache + "/years.json", yearsJson)

// Save the data points in binary format
let writeData file (data:seq<DataPoint>) = 
  use fs = new FileStream(file, FileMode.Create)
  let write (bytes:byte[]) = fs.Write(bytes, 0, bytes.Length) 
  for v in data do 
    System.BitConverter.GetBytes(v.Country) |> write
    System.BitConverter.GetBytes(v.Indicator) |> write
    System.BitConverter.GetBytes(v.Year) |> write
    System.BitConverter.GetBytes(v.Value) |> write

data |> writeData (cache + "/worldbank.dat")
