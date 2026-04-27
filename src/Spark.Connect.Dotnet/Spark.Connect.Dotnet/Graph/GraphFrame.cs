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

    public GraphFrame(DataFrame vertices, DataFrame edges)
    {
        Vertices = vertices;
        Edges = edges;
    }

    public DataFrame OutDegrees => Edges.GroupBy("src").Count()
        .WithColumnRenamed("src", "id")
        .WithColumnRenamed("count", "outDegree");

    public DataFrame InDegrees => Edges.GroupBy("dst").Count()
        .WithColumnRenamed("dst", "id")
        .WithColumnRenamed("count", "inDegree");

    public DataFrame Degrees => Edges.Select(Col("src").Alias("id"))
        .Union(Edges.Select(Col("dst").Alias("id")))
        .GroupBy("id")
        .Count();

    /// <summary>
    /// Local join-based triplets (src vertex, edge, dst vertex).
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
        return Execute(new GraphFramesAPI { Triplets = new Triplets() });
    }

    public DataFrame Find(string pattern)
    {
        return Execute(new GraphFramesAPI
        {
            Find = new Find { Pattern = pattern }
        });
    }

    public DataFrame FilterEdges(string condition)
    {
        return Execute(new GraphFramesAPI
        {
            FilterEdges = new FilterEdges
            {
                Condition = new ColumnOrExpression { Expr = condition }
            }
        });
    }

    public DataFrame FilterEdges(Column condition)
    {
        return Execute(new GraphFramesAPI
        {
            FilterEdges = new FilterEdges
            {
                Condition = new ColumnOrExpression { Col = condition.Expression.ToByteString() }
            }
        });
    }

    public DataFrame FilterVertices(string condition)
    {
        return Execute(new GraphFramesAPI
        {
            FilterVertices = new FilterVertices
            {
                Condition = new ColumnOrExpression { Expr = condition }
            }
        });
    }

    public DataFrame FilterVertices(Column condition)
    {
        return Execute(new GraphFramesAPI
        {
            FilterVertices = new FilterVertices
            {
                Condition = new ColumnOrExpression { Col = condition.Expression.ToByteString() }
            }
        });
    }

    public DataFrame DropIsolatedVertices()
    {
        return Execute(new GraphFramesAPI { DropIsolatedVertices = new DropIsolatedVertices() });
    }

    public DataFrame Bfs(string fromExpression, string toExpression, int maxPathLength = 10, string? edgeFilter = null)
    {
        return Execute(new GraphFramesAPI
        {
            Bfs = new BFS
            {
                FromExpr = new ColumnOrExpression { Expr = fromExpression },
                ToExpr = new ColumnOrExpression { Expr = toExpression },
                EdgeFilter = new ColumnOrExpression { Expr = string.IsNullOrEmpty(edgeFilter) ? "true" : edgeFilter },
                MaxPathLength = maxPathLength
            }
        });
    }

    /// <param name="resetProbability">Probability of resetting to a random vertex (default 0.15).</param>
    /// <param name="maxIterations">Maximum number of iterations. Mutually exclusive with <paramref name="tolerance"/>.</param>
    /// <param name="tolerance">Convergence tolerance. Mutually exclusive with <paramref name="maxIterations"/>.</param>
    /// <param name="sourceLongId">Run personalized PageRank from this long vertex ID. Mutually exclusive with <paramref name="sourceStringId"/>.</param>
    /// <param name="sourceStringId">Run personalized PageRank from this string vertex ID. Mutually exclusive with <paramref name="sourceLongId"/>.</param>
    public DataFrame PageRank(double resetProbability = 0.15, int? maxIterations = null, double? tolerance = null,
        long? sourceLongId = null, string? sourceStringId = null)
    {
        if (maxIterations.HasValue && tolerance.HasValue)
            throw new ArgumentException("PageRank: specify either maxIterations or tolerance, not both.");

        if (sourceLongId.HasValue && !string.IsNullOrEmpty(sourceStringId))
            throw new ArgumentException("PageRank: specify source id as either long or string, not both.");

        var pageRank = new PageRank { ResetProbability = resetProbability };

        if (maxIterations.HasValue)
            pageRank.MaxIter = maxIterations.Value;

        if (tolerance.HasValue)
            pageRank.Tol = tolerance.Value;

        if (sourceLongId.HasValue)
            pageRank.SourceId = new StringOrLongID { LongId = sourceLongId.Value };
        else if (!string.IsNullOrEmpty(sourceStringId))
            pageRank.SourceId = new StringOrLongID { StringId = sourceStringId };

        return Execute(new GraphFramesAPI { PageRank = pageRank });
    }

    public DataFrame ParallelPersonalizedPageRank(IEnumerable<string> sourceIds, double resetProbability = 0.15, int maxIter = 10)
    {
        var pppr = new ParallelPersonalizedPageRank
        {
            ResetProbability = resetProbability,
            MaxIter = maxIter
        };
        pppr.SourceIds.AddRange(sourceIds.Select(id => new StringOrLongID { StringId = id }));
        return Execute(new GraphFramesAPI { ParallelPersonalizedPageRank = pppr });
    }

    public DataFrame ParallelPersonalizedPageRank(IEnumerable<long> sourceIds, double resetProbability = 0.15, int maxIter = 10)
    {
        var pppr = new ParallelPersonalizedPageRank
        {
            ResetProbability = resetProbability,
            MaxIter = maxIter
        };
        pppr.SourceIds.AddRange(sourceIds.Select(id => new StringOrLongID { LongId = id }));
        return Execute(new GraphFramesAPI { ParallelPersonalizedPageRank = pppr });
    }

    public DataFrame ConnectedComponents(string algorithm = "graphframes", int checkpointInterval = 2, int broadcastThreshold = 1_000_000)
    {
        return Execute(new GraphFramesAPI
        {
            ConnectedComponents = new ConnectedComponents
            {
                Algorithm = algorithm,
                CheckpointInterval = checkpointInterval,
                BroadcastThreshold = broadcastThreshold
            }
        });
    }

    public DataFrame StronglyConnectedComponents(int maxIter = 10)
    {
        return Execute(new GraphFramesAPI
        {
            StronglyConnectedComponents = new StronglyConnectedComponents { MaxIter = maxIter }
        });
    }

    public DataFrame LabelPropagation(int maxIter = 5)
    {
        return Execute(new GraphFramesAPI
        {
            LabelPropagation = new LabelPropagation { MaxIter = maxIter }
        });
    }

    public DataFrame ShortestPaths(IEnumerable<string> landmarks, string algorithm = "graphframes")
    {
        var sp = new ShortestPaths { Algorithm = algorithm };
        sp.Landmarks.AddRange(landmarks.Select(l => new StringOrLongID { StringId = l }));
        return Execute(new GraphFramesAPI { ShortestPaths = sp });
    }

    public DataFrame ShortestPaths(IEnumerable<long> landmarks, string algorithm = "graphframes")
    {
        var sp = new ShortestPaths { Algorithm = algorithm };
        sp.Landmarks.AddRange(landmarks.Select(l => new StringOrLongID { LongId = l }));
        return Execute(new GraphFramesAPI { ShortestPaths = sp });
    }

    public DataFrame ShortestPaths(IEnumerable<int> landmarks, string algorithm = "graphframes")
    {
        var sp = new ShortestPaths { Algorithm = algorithm };
        sp.Landmarks.AddRange(landmarks.Select(l => new StringOrLongID { LongId = l }));
        return Execute(new GraphFramesAPI { ShortestPaths = sp });
    }

    public DataFrame TriangleCount()
    {
        return Execute(new GraphFramesAPI { TriangleCount = new TriangleCount() });
    }

    public DataFrame DetectingCycles()
    {
        return Execute(new GraphFramesAPI { DetectingCycles = new DetectingCycles() });
    }

    public DataFrame KCore()
    {
        return Execute(new GraphFramesAPI { Kcore = new KCore() });
    }

    public DataFrame MaximalIndependentSet(long seed = 0)
    {
        return Execute(new GraphFramesAPI
        {
            Mis = new MaximalIndependentSet { Seed = seed }
        });
    }

    public DataFrame PowerIterationClustering(int k, int maxIter = 20, string? weightCol = null)
    {
        var pic = new PowerIterationClustering { K = k, MaxIter = maxIter };
        if (!string.IsNullOrEmpty(weightCol))
            pic.WeightCol = weightCol;
        return Execute(new GraphFramesAPI { PowerIterationClustering = pic });
    }

    public DataFrame SvdPlusPlus(int rank = 10, int maxIter = 2, double minValue = 0.0, double maxValue = 5.0,
        double gamma1 = 0.007, double gamma2 = 0.007, double gamma6 = 0.005, double gamma7 = 0.015)
    {
        return Execute(new GraphFramesAPI
        {
            SvdPlusPlus = new SVDPlusPlus
            {
                Rank = rank,
                MaxIter = maxIter,
                MinValue = minValue,
                MaxValue = maxValue,
                Gamma1 = gamma1,
                Gamma2 = gamma2,
                Gamma6 = gamma6,
                Gamma7 = gamma7
            }
        });
    }

    private DataFrame Execute(GraphFramesAPI api)
    {
        api.Vertices = new Plan { Root = Vertices.Relation }.ToByteString();
        api.Edges = new Plan { Root = Edges.Relation }.ToByteString();

        return new DataFrame(Vertices.SparkSession, new Relation
        {
            Extension = Any.Pack(api)
        });
    }
}
