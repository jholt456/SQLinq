using System;
using System.Linq;
using System.Linq.Expressions;
using SQLinq.Compiler;

namespace SQLinq
{
    public class SQLinqJoin<TResult, TParent> : SQLinq<TResult>
    {

        public SQLinqJoin(SQLinq<TParent> parent) : base(parent.GetTableName(true), parent.Dialect)
        {
            this.Parent = parent;
        }

        public override ISQLinqResult ToSQL(int existingParameterCount = 0, string parameterNamePrefix = SqlExpressionCompiler.DefaultParameterNamePrefix)
        {
            var local = this as ITypedSqlLinq;

            var newItem = new SQLinq<TResult>(this.TableNameOverride, this.Dialect);

            while (local != null)
            {
                MapJoins(local, newItem);

                newItem.Expressions.AddRange(MapExpressions(local.Expressions, newItem.JoinExpressions));

                local = local.Parent;
            }
            
            newItem.Select(this.Selector);

            newItem.JoinExpressions.Reverse();
            newItem.Expressions.Reverse();
            var sql = (SQLinqSelectResult)newItem.ToSQL(existingParameterCount, parameterNamePrefix);

            //replace the select
            if (sql.Select.Length == 1 && sql.Select[0] == "*")
            {
                var resultSelector = newItem.JoinExpressions.Last().Process(sql.Parameters, parameterNamePrefix).Results.Select.ToArray();

                if (resultSelector.Any())
                {
                    sql.Select = resultSelector;
                }
            }

            return sql;
        }

        private static void MapJoins(ITypedSqlLinq local, SQLinq<TResult> newItem)
        {
            if (local.JoinExpressions.Any())
            {
                newItem.JoinExpressions.AddRange(local.JoinExpressions);
            }
        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <returns>The SQLinq instance to allow for method chaining.</returns>
        public new SQLinq<TResult> Select(Expression<Func<TResult, object>> selector)
        {
            this.Selector = selector;
            return this;
        }
    }

    public class PredicateRewriterVisitor : ExpressionVisitor
    {
        private readonly ParameterExpression _parameterExpression;

        public PredicateRewriterVisitor(ParameterExpression parameterExpression)
        {
            _parameterExpression = parameterExpression;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            return _parameterExpression;
        }
    }
}