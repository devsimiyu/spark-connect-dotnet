using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Graph.Connect;
using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Graph;

public class GraphFrame
{
    public DataFrame Vertices { get; }
    public DataFrame Edges { get; }

    private ByteString _verticesByteString => new Plan 
        {
            Root = Vertices.Relation
        }
        .ToByteString();

    private ByteString _edgesByteString => new Plan
        {
            Root = Edges.Relation
        }
        .ToByteString();

    public GraphFrame(DataFrame vertices, DataFrame edges)
    {
        Vertices = vertices;
        Edges = edges;
    }

    /// <summary>
    /// The out-degree of each vertex in the graph.
    /// </summary>
    public DataFrame OutDegrees => Edges.GroupBy("src").Count()
        .WithColumnRenamed("src", "id")
        .WithColumnRenamed("count", "outDegree");

    /// <summary>
    /// The in-degree of each vertex in the graph.
    /// </summary>
    public DataFrame InDegrees => Edges.GroupBy("dst").Count()
        .WithColumnRenamed("dst", "id")
        .WithColumnRenamed("count", "inDegree");

    /// <summary>
    /// The degree of each vertex in the graph.
    /// </summary>
    public DataFrame Degrees => Edges.Select(Col("src").Alias("id"))
        .Union(Edges.Select(Col("dst").Alias("id")))
        .GroupBy("id")
        .Count();

    /// <summary>
    /// The triplets of the graph.
    /// </summary>
    public DataFrame Triplet
    {
        get
        {
            var vSrc = Vertices.Select(Col("*")).Alias("src");
            var vDst = Vertices.Select(Col("*")).Alias("dst");
            var e = Edges.Alias("edge");

            return e.Join(vSrc, Col("edge.src") == Col("src.id"))
                .Join(vDst, Col("edge.dst") == Col("dst.id"));
        }
    }

    public DataFrame Triplets()
    {
        var graphFramesApi = new GraphFramesAPI
        {
            Vertices = _verticesByteString,
            Edges = _edgesByteString,
            Triplets = new Triplets()
        };

        return Execute(graphFramesApi);
    }

    public DataFrame Find(string pattern)
    {
        var graphFramesApi = new GraphFramesAPI
        {
            Vertices = _verticesByteString,
            Edges = _edgesByteString,
            Find = new Find
            {
                Pattern = pattern
            }
        };

        return Execute(graphFramesApi);
    }
    
    public DataFrame Bfs(string fromExpression, string toExpression, int? maxPathLength, string? edgeFilter)
    {
        var graphFramesApi = new GraphFramesAPI
        {
            Vertices = _verticesByteString,
            Edges = _edgesByteString,
            Bfs = new BFS
            {
                FromExpr = new ColumnOrExpression
                {
                    Expr = fromExpression
                },
                ToExpr = new ColumnOrExpression
                {
                    Expr = toExpression,
                    Col = ByteString.CopyFromUtf8(toExpression)
                },
                EdgeFilter = new ColumnOrExpression
                {
                    Expr = string.IsNullOrEmpty(edgeFilter) ? "true" : edgeFilter
                },
                MaxPathLength = maxPathLength ?? 0
            }
        };

        return Execute(graphFramesApi);
    }

    public DataFrame PageRank(double resetProbability, int? maxIterations, double? tolerance, long? sourceLongId, string? sourceStringId)
    {
        if ((maxIterations.HasValue && tolerance.HasValue) ||
            (!maxIterations.HasValue && !tolerance.HasValue))
        {
            throw new ArgumentException("PageRank: specify either maximum iteration or tolerance, not both.");
        }

        if ((sourceLongId.HasValue && !string.IsNullOrEmpty(sourceStringId)) ||
            (!sourceLongId.HasValue && string.IsNullOrEmpty(sourceStringId)))
        {
            throw new ArgumentException("PageRank: specify source id as either long or string, not both.");
        }
        
        var graphFramesApi = new GraphFramesAPI
        {
            Vertices = _verticesByteString,
            Edges = _edgesByteString,
            PageRank = new PageRank
            {
                MaxIter = maxIterations ?? 0,
                ResetProbability = resetProbability,
                Tol = tolerance ?? 0,
                SourceId = new StringOrLongID
                {
                    LongId = sourceLongId ?? 0,
                    StringId = sourceStringId ?? string.Empty
                }
            }
        };

        return Execute(graphFramesApi);
    }

    private DataFrame Execute(GraphFramesAPI graphFramesApi)
    {
        var relation = new Relation
        {
            Extension = Any.Pack(graphFramesApi)
        };
        
        return new DataFrame(Vertices.SparkSession, relation);
    }
}
