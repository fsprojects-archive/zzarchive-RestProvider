module WorldBank.Domain
open System.Collections.Generic
open Newtonsoft.Json
open System.IO

[<Struct>]
type DataPoint(country:uint16, indicator:uint16, year:uint16, value:float) =
  member x.Country = country
  member x.Indicator = indicator
  member x.Year = year
  member x.Value = value

type Country = 
  { Index : uint16 
    ID : string
    Iso2Code : string
    Name : string
    Capital : string 
    RegionId : string 
    RegionName : string
    IncomeLevel : string 
    DataPoints : int }

type Indicator = 
  { Index : uint16
    ID : string
    Name : string
    Source : string
    SourceNote : string
    SourceOrg : string
    Topics : string list
    DataPoints : int }

type Year = 
  { Year : string
    Index : uint16 
    DataPoints : int }

type Topic = 
  { Indicators : seq<string>
    Name : string }  

type Data =
  { Countries : IDictionary<string, Country>
    Indicators : IDictionary<string, Indicator>
    Years : IDictionary<string, Year>
    CountriesByIndex : IDictionary<uint16, Country>
    IndicatorsByIndex : IDictionary<uint16, Indicator>
    YearsByIndex : IDictionary<uint16, Year>
    Data : IReadOnlyCollection<DataPoint> 
    Topics : IDictionary<string, Topic> }


let readCache folder = 
  let serializer = JsonSerializer.Create()

  let fromJson json : 'T = 
    use tr = new System.IO.StringReader(json)
    serializer.Deserialize<'T>(new JsonTextReader(tr))

  let readData file = seq {
    use fs = new FileStream(file, FileMode.Open)
    let block = Array.zeroCreate 14
    let finished = ref false
    fs.Read(block, 0, 14) |> ignore
    while not finished.Value do
      yield 
        DataPoint
          ( System.BitConverter.ToUInt16(block, 0),
            System.BitConverter.ToUInt16(block, 2),
            System.BitConverter.ToUInt16(block, 4),
            System.BitConverter.ToDouble(block, 6) )
      finished := (fs.Read(block, 0, 14) = 0) }

  let countries = File.ReadAllText(folder + "/countries.json") |> fromJson |> dict 
  let indicators = File.ReadAllText(folder + "/indicators.json") |> fromJson |> dict 
  let years = File.ReadAllText(folder + "/years.json") |> fromJson |> dict 
  let data = readData (folder + "/worldbank.dat") |> Array.ofSeq 
  
  let indicatorsForTopic topic = 
    indicators 
    |> Seq.filter (fun (KeyValue(k, { Indicator.Topics = t })) -> 
        t |> Seq.exists (fun topic' -> topic = topic'))
    |> Seq.map (fun (KeyValue(k, _)) -> k)

  let topics =
    indicators.Values 
    |> Seq.collect (fun { Indicator.Topics = t } -> t) 
    |> Seq.distinct
    |> Seq.mapi (fun i t ->
        sprintf "topic_%d" i, 
        { Name = t.Trim(); Indicators = indicatorsForTopic t })
  let topics = 
    Seq.append [ "all", { Name = "All Indicators"; Indicators = indicators.Keys } ] topics

  let countriesByIdx = dict [ for c:Country in countries.Values -> c.Index, c ]
  let indicatorsByIdx = dict [ for c:Indicator in indicators.Values -> c.Index, c ]
  let yearsByIdx = dict [ for c:Year in years.Values -> c.Index, c ]
            
  { Countries = countries; Indicators = indicators; CountriesByIndex = countriesByIdx; 
    IndicatorsByIndex = indicatorsByIdx; YearsByIndex = yearsByIdx    
    Years = years; Topics = dict topics; Data = data }
