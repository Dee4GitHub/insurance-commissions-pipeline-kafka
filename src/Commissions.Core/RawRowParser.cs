using System.Globalization;
namespace Commissions.Core;

public static class RawRowParser
{
    public static RawRow Parse(string line, long rowId, string batchId)
    {
        var parts = line.Split(',');
        var rawRow = new RawRow();

        rawRow.RowId = rowId.ToString();
        rawRow.BatchId = batchId;
        rawRow.RawLine = line;
        rawRow.LineNumber = rowId;

        if (parts.Length != 5)
        {
            rawRow.IsParseable = false;
            rawRow.ParseError = $"Expected 5 parts but got {parts.Length}";

            return rawRow;
        }

        if (string.IsNullOrWhiteSpace(parts[0]))
        {
            rawRow.IsParseable = false;
            rawRow.ParseError = "RowId is empty";
            return rawRow;
        }

        rawRow.RowId = parts[0].Trim();

        if (parts[1].Trim() == string.Empty)
        {
            rawRow.IsParseable = false;
            rawRow.ParseError = "BrokerId is empty";
            return rawRow;
        }

        if(parts[2].Trim()== string.Empty)
        {
            rawRow.IsParseable = false;
            rawRow.ParseError = "PolicyNumber is empty";
            return rawRow;
        }

        rawRow.BrokerId = parts[1].Trim();
        rawRow.PolicyNumber = parts[2].Trim();

        if (!decimal.TryParse(parts[3].Trim(),CultureInfo.InvariantCulture, out var premiumAmount))
        {
            rawRow.IsParseable = false;
            rawRow.ParseError = $"Invalid PremiumAmount: {parts[3].Trim()}";
            return rawRow;
        }
        else
        {
            rawRow.PremiumAmount = premiumAmount;
        }

        if (!decimal.TryParse(parts[4].Trim(), CultureInfo.InvariantCulture, out var commissionRate))
        {
            rawRow.IsParseable = false;
            rawRow.ParseError = $"Invalid CommissionRate: {parts[4].Trim()}";
            return rawRow;
        }
        else
        {
            rawRow.CommissionRate = commissionRate;
        }
        rawRow.IsParseable = true;
        return rawRow;
    }
}