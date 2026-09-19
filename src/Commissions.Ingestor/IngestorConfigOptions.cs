namespace Commissions.Ingestor;
public class IngestorConfigOptions
{
    public const string SectionName = "Ingestion";
    public string CsvFilePath { get; set; } = default!;
    public int FileChunkSize { get; set; }
}