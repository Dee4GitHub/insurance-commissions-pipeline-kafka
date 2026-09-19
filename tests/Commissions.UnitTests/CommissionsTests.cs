namespace Commissions.Tests;
public class CommissionsTests
{
    [Fact]
    public void TestCommissionProcessing_Case1()
    {
        // Arrange
        //10,B100,POL-0010,878.46,0.15
        var rawCommission = new CommissionRaw(
            RowId: "10",
            BrokerId: "B100",
            PolicyNumber: "POL-0010",
            PremiumAmount: 878.46m,
            CommissionRate: 0.15m,
            BatchId: "batch101"
        );


        // Act
        var result = CommissionCalculator.CalculateCommission(rawCommission.PremiumAmount, rawCommission.CommissionRate, 1.0m);

        // Assert
        Assert.Equal(131.77m, result);
    }

    [Fact]
    public void TestCommissionProcessing_Case2()
    {
        // Arrange
        //3,B100,POL-0003,3989.3,0.15
        var rawCommission = new CommissionRaw(
            RowId: "3",
            BrokerId: "B100",
            PolicyNumber: "POL-0003",
            PremiumAmount: 3989.3m,
            CommissionRate: 0.15m,
            BatchId: "batch102"
        );

        // Act
        var result = CommissionCalculator.CalculateCommission(rawCommission.PremiumAmount, rawCommission.CommissionRate, 1.0m);

        // Assert
        Assert.Equal(598.40m, result);
    }
    [Theory]
    [InlineData("")]
    [InlineData("3,B100")]
    [InlineData("3,B100,POL-0003,3989.3,0.15,EXTRA")]
    [InlineData("3,B100,POL-0003,3989.3,NOT_A_NUMBER")]
    [InlineData("3,B100,POL-0003,3989.3,0.15,")]
    public void Parse_RejectsMalformedLine(string line)
    {
        var result = RawRowParser.Parse(line, 1, "batch1");
        Assert.False(result.IsParseable);
        Assert.NotNull(result.ParseError);
    }

    [Theory]
    [InlineData("3,B100,POL-0003,3989.3,0.15", "B100", "POL-0003", 3989.3, 0.15)]
    [InlineData("  3 , B100 , POL-0003 , 3989.3 , 0.15  ", "B100", "POL-0003", 3989.3, 0.15)]
    public void Parse_ReadsFields(string line, string brokerId, string policy, double premium, double rate)
    {
        var result = RawRowParser.Parse(line, 1, "batch1");
        Assert.True(result.IsParseable);
        Assert.Null(result.ParseError);
        Assert.Equal(brokerId, result.BrokerId);
        Assert.Equal(policy, result.PolicyNumber);
        Assert.Equal((decimal)premium, result.PremiumAmount);
        Assert.Equal((decimal)rate, result.CommissionRate);
    }

    [Fact]
    public void Parse_PreservesOriginalLine_WhenRejected()
    {
        const string line = "3,B100";   // too few fields

        var result = RawRowParser.Parse(line, 1, "batch1");

        Assert.False(result.IsParseable);
        Assert.Equal(line, result.RawLine);
    }
}