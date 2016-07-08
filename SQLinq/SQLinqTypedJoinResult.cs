using System.Collections.Generic;
using SQLinq.Compiler;

namespace SQLinq
{
    public class SQLinqTypedJoinResult 
    {
        private readonly string innerTable;
        private readonly SqlExpressionCompilerSelectorResult inner;
        private readonly SqlExpressionCompilerSelectorResult outer;
        private readonly SqlExpressionCompilerSelectorResult results;

        public SQLinqTypedJoinResult(string innerTable, SqlExpressionCompilerSelectorResult inner, SqlExpressionCompilerSelectorResult outer, SqlExpressionCompilerSelectorResult results, IDictionary<string, object> parameters)
        {
            this.innerTable = innerTable;
            this.inner = inner;
            this.outer = outer;
            this.results = results;
        }

        public string ToQuery()
        {
            return string.Format("JOIN {0} ON {2} = {1}", innerTable, inner.Select[0], outer.Select[0]);
        }

        public IEnumerable<string> Join { get; set; }
        public IDictionary<string, object> Parameters { get; set; }

        public SqlExpressionCompilerSelectorResult Results
        {
            get { return results; }
        }
    }
}