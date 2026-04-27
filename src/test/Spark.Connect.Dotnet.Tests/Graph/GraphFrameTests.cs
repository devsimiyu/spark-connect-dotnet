using Spark.Connect.Dotnet.Graph;
using Xunit.Abstractions;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.Graph;

/// <summary>
/// Graph (Directed):
///
/// Vertices (INT id):  1 - Alice(34)   2 - Bob(36)   3 - Charlie(30)   4 - Anne(29)
///
/// Edges (INT src/dst): 1 -> 2 friend
///                      2 -> 3 follow
///                      3 -> 1 friend    &lt;- closes triangle 1 -> 2 -> 3 -> 1
///                      1 -> 4 colleague &lt;- 4 is a leaf
///
/// ids are plain INT (INT32), not BIGINT. ShortestPaths internally builds
/// MAP&lt;id_type, INT&gt; and requires INT32 ids to avoid a type mismatch.
/// </summary>
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

    private static List<string> Columns(Dotnet.Sql.DataFrame df)
    {
        var rows = df.Collect();
        return rows.Count == 0
            ? []
            : rows[0].Schema.Fields.Select(f => f.Name).ToList();
    }

    private static List<T> ColumnValues<T>(Dotnet.Sql.DataFrame df, string columnName)
    {
        return df.Collect().Select(r => (T)r.Get(columnName)).ToList();
    }

    // -------------------------------------------------------------------------
    // PageRank
    // -------------------------------------------------------------------------

    [Fact]
    public void PageRank_HasPageRankColumn()
    {
        Assert.Contains("pagerank", Columns(_graphFrame.PageRank(maxIterations: 5)));
    }

    [Fact]
    public void PageRank_ReturnsOneRowPerVertex()
    {
        Assert.Equal(4, _graphFrame.PageRank(maxIterations: 5).Collect().Count);
    }

    [Fact]
    public void PageRank_AllValuesPositive()
    {
        var ranks = ColumnValues<double>(_graphFrame.PageRank(maxIterations: 5), "pagerank");
        Assert.All(ranks, r => Assert.True(r > 0.0));
    }

    [Fact]
    public void PageRank_ThrowsWhenBothMaxIterAndTolProvided()
    {
        Assert.Throws<ArgumentException>(() =>
            _graphFrame.PageRank(maxIterations: 5, tolerance: 0.01));
    }

    [Fact]
    public void PageRank_WithTolConverges()
    {
        Assert.Equal(4, _graphFrame.PageRank(tolerance: 0.01).Collect().Count);
    }

    // -------------------------------------------------------------------------
    // Find (Motif Matching)
    // -------------------------------------------------------------------------

    [Fact]
    public void Find_SingleHopMatchesAllEdges()
    {
        Assert.Equal(4, _graphFrame.Find("(a)-[e]->(b)").Collect().Count);
    }

    [Fact]
    public void Find_TriangleReturnsThreeRotations()
    {
        // GraphFrames returns one row per rotation of each triangle.
        // The single triangle 1 -> 2 -> 3 -> 1 produces 3 rows:
        //   (a=1,b=2,c=3), (a=2,b=3,c=1), (a=3,b=1,c=2)
        Assert.Equal(3, _graphFrame.Find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)").Collect().Count);
    }

    [Fact]
    public void Find_TwoHopReturnsResults()
    {
        Assert.True(_graphFrame.Find("(a)-[e1]->(b); (b)-[e2]->(c)").Collect().Count > 0);
    }

    [Fact]
    public void Find_ResultHasCorrectColumns()
    {
        var cols = Columns(_graphFrame.Find("(a)-[e]->(b)"));
        Assert.Contains("a", cols);
        Assert.Contains("e", cols);
        Assert.Contains("b", cols);
    }

    // -------------------------------------------------------------------------
    // Triplets
    // -------------------------------------------------------------------------

    [Fact]
    public void Triplets_CountMatchesEdges()
    {
        Assert.Equal(4, _graphFrame.Triplets().Collect().Count);
    }

    [Fact]
    public void Triplets_HasCorrectColumns()
    {
        var cols = Columns(_graphFrame.Triplets());
        Assert.Contains("src", cols);
        Assert.Contains("edge", cols);
        Assert.Contains("dst", cols);
    }

    // -------------------------------------------------------------------------
    // FilterEdges
    // -------------------------------------------------------------------------

    [Fact]
    public void FilterEdges_SQL_KeepsFriendEdges()
    {
        Assert.Equal(2, _graphFrame.FilterEdges("relationship = 'friend'").Collect().Count);
    }

    [Fact]
    public void FilterEdges_Column_KeepsFriendEdges()
    {
        Assert.Equal(2, _graphFrame.FilterEdges(Col("relationship") == "friend").Collect().Count);
    }

    [Fact]
    public void FilterEdges_NoMatchReturnsEmpty()
    {
        Assert.Empty(_graphFrame.FilterEdges("relationship = 'enemy'").Collect());
    }

    // -------------------------------------------------------------------------
    // FilterVertices
    // -------------------------------------------------------------------------

    [Fact]
    public void FilterVertices_SQL_KeepsYoungVertices()
    {
        // age < 34: Charlie(30), Anne(29) -> 2 vertices
        Assert.Equal(2, _graphFrame.FilterVertices("age < 34").Collect().Count);
    }

    [Fact]
    public void FilterVertices_Column_KeepsYoungVertices()
    {
        Assert.Equal(2, _graphFrame.FilterVertices(Col("age") < 34).Collect().Count);
    }

    [Fact]
    public void FilterVertices_NoMatchReturnsEmpty()
    {
        Assert.Empty(_graphFrame.FilterVertices("age > 100").Collect());
    }

    // -------------------------------------------------------------------------
    // DropIsolatedVertices
    // -------------------------------------------------------------------------

    [Fact]
    public void DropIsolatedVertices_KeepsAllWhenNoneIsolated()
    {
        Assert.Equal(4, _graphFrame.DropIsolatedVertices().Collect().Count);
    }

    [Fact]
    public void DropIsolatedVertices_RemovesIsolatedNode()
    {
        var verticesWithIsolated = Spark.Sql(@"
            SELECT CAST(id AS INT) AS id, name, age FROM VALUES
                (1,  'Alice',   34),
                (2,  'Bob',     36),
                (3,  'Charlie', 30),
                (4,  'Anne',    29),
                (99, 'Ghost',   99)
            AS people(id, name, age)
        ");
        Assert.Equal(4, new GraphFrame(verticesWithIsolated, _edges).DropIsolatedVertices().Collect().Count);
    }

    // -------------------------------------------------------------------------
    // BFS
    // -------------------------------------------------------------------------

    [Fact]
    public void Bfs_FindsPath()
    {
        Assert.True(_graphFrame.Bfs("id = 1", "id = 3").Collect().Count > 0);
    }

    [Fact]
    public void Bfs_NoPathInDirectedGraph()
    {
        // Vertex 4 has no outgoing edges so it cannot reach vertex 1.
        Assert.Empty(_graphFrame.Bfs("id = 4", "id = 1").Collect());
    }

    [Fact]
    public void Bfs_WithEdgeFilter()
    {
        Assert.True(_graphFrame.Bfs("id = 1", "id = 2", edgeFilter: "relationship = 'friend'").Collect().Count > 0);
    }

    // -------------------------------------------------------------------------
    // ConnectedComponents
    // -------------------------------------------------------------------------

    [Fact]
    public void ConnectedComponents_ReturnsOneRowPerVertex()
    {
        Assert.Equal(4, _graphFrame.ConnectedComponents().Collect().Count);
    }

    [Fact]
    public void ConnectedComponents_HasComponentColumn()
    {
        Assert.Contains("component", Columns(_graphFrame.ConnectedComponents()));
    }

    [Fact]
    public void ConnectedComponents_AllInSameComponent()
    {
        // All 4 vertices are reachable from each other (undirected sense).
        var components = ColumnValues<long>(_graphFrame.ConnectedComponents(), "component");
        Assert.Single(components.Distinct());
    }

    // -------------------------------------------------------------------------
    // StronglyConnectedComponents
    // -------------------------------------------------------------------------

    [Fact]
    public void StronglyConnectedComponents_ReturnsOneRowPerVertex()
    {
        Assert.Equal(4, _graphFrame.StronglyConnectedComponents().Collect().Count);
    }

    [Fact]
    public void StronglyConnectedComponents_HasComponentColumn()
    {
        Assert.Contains("component", Columns(_graphFrame.StronglyConnectedComponents()));
    }

    [Fact]
    public void StronglyConnectedComponents_ProducesMultipleComponents()
    {
        // Triangle 1-2-3 is one SCC; vertex 4 (leaf) is its own SCC.
        var components = ColumnValues<long>(_graphFrame.StronglyConnectedComponents(), "component");
        Assert.True(components.Distinct().Count() > 1);
    }

    // -------------------------------------------------------------------------
    // ShortestPaths
    // -------------------------------------------------------------------------

    [Fact]
    public void ShortestPaths_ReturnsOneRowPerVertex()
    {
        Assert.Equal(4, _graphFrame.ShortestPaths(new[] { 1, 3 }).Collect().Count);
    }

    [Fact]
    public void ShortestPaths_HasDistancesColumn()
    {
        Assert.Contains("distances", Columns(_graphFrame.ShortestPaths(new[] { 1 })));
    }

    // -------------------------------------------------------------------------
    // TriangleCount
    // -------------------------------------------------------------------------

    [Fact]
    public void TriangleCount_ReturnsOneRowPerVertex()
    {
        Assert.Equal(4, _graphFrame.TriangleCount().Collect().Count);
    }

    [Fact]
    public void TriangleCount_HasCountColumn()
    {
        Assert.Contains("count", Columns(_graphFrame.TriangleCount()));
    }

    [Fact]
    public void TriangleCount_VerticesInTriangleAreNonZero()
    {
        var rows = _graphFrame.TriangleCount().Collect();
        var counts = rows.ToDictionary(r => (int)r.Get("id"), r => (long)r.Get("count"));

        Assert.True(counts[1] > 0);
        Assert.True(counts[2] > 0);
        Assert.True(counts[3] > 0);
    }

    [Fact]
    public void TriangleCount_LeafVertexIsZero()
    {
        // Vertex 4 (Anne) is a leaf — not part of any triangle.
        var rows = _graphFrame.TriangleCount().Collect();
        var counts = rows.ToDictionary(r => (int)r.Get("id"), r => (long)r.Get("count"));

        Assert.Equal(0L, counts[4]);
    }

    // -------------------------------------------------------------------------
    // LabelPropagation
    // -------------------------------------------------------------------------

    [Fact]
    public void LabelPropagation_ReturnsOneRowPerVertex()
    {
        Assert.Equal(4, _graphFrame.LabelPropagation().Collect().Count);
    }

    [Fact]
    public void LabelPropagation_HasLabelColumn()
    {
        Assert.Contains("label", Columns(_graphFrame.LabelPropagation()));
    }

    // -------------------------------------------------------------------------
    // Chaining
    // -------------------------------------------------------------------------

    [Fact]
    public void Chain_FindThenFilter()
    {
        var result = _graphFrame.Find("(a)-[e]->(b)").Filter("e.relationship = 'friend'");
        Assert.True(result.Collect().Count > 0);
    }

    [Fact]
    public void Chain_PageRankThenFilter()
    {
        var result = _graphFrame.PageRank(maxIterations: 5).Filter("pagerank > 0.0");
        Assert.True(result.Collect().Count > 0);
    }

    [Fact]
    public void Chain_PageRankOnSubgraph()
    {
        var subV = _vertices.Filter("age >= 30");
        var subE = _edges.Filter("src IN (1,2,3) AND dst IN (1,2,3)");
        Assert.Equal(3, new GraphFrame(subV, subE).PageRank(maxIterations: 3).Collect().Count);
    }
}
