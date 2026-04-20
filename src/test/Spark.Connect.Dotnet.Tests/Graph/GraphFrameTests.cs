using Spark.Connect.Dotnet.Graph;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.Graph;

public class GraphFrameTests : E2ETestBase
{
    private readonly Dotnet.Sql.DataFrame _vertices;
    private readonly Dotnet.Sql.DataFrame _edges;
    private readonly GraphFrame _graphFrame;
    
    public GraphFrameTests(ITestOutputHelper logger) : base(logger)
    {
        _vertices = Spark.Sql(@"
            SELECT CAST(id AS INT) AS id, name, age FROM VALUES
                (1, 'Alice',   34),
                (2, 'Bob',     36),
                (3, 'Charlie', 30),
                (4, 'Anne',    29)
            AS people(id, name, age)
        ");

        _edges = Spark.Sql(@"
            SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst, relationship FROM VALUES
                (1, 2, 'friend'),
                (2, 3, 'follow'),
                (3, 1, 'friend'),
                (1, 4, 'colleague')
            AS connections(src, dst, relationship)
        ");

        _graphFrame = new GraphFrame(_vertices, _edges);
    }

    /// <summary>
    /// Motif matching
    /// </summary>
    [Fact]
    public void GraphFrame_Find()
    {
        var pattern = "(a)-[e]->(b)";
        
        var dataFrame = _graphFrame.Find(pattern);
        var result = dataFrame.Collect();
        
        Assert.NotEmpty(result);
        Assert.Equal(4, result.Count);
    }
}
