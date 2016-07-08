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

                MapExpressions(local, newItem);

                local = local.Parent;
            }
            
            newItem.Select(this.Selector);

            newItem.JoinExpressions.Reverse();
            newItem.Expressions.Reverse();
            var sql = (SQLinqSelectResult)newItem.ToSQL(existingParameterCount, parameterNamePrefix);

            //replace the select
            if (sql.Select.Length == 1 && sql.Select[0] == "*")
            {
                sql.Select = newItem.JoinExpressions.Last().Process(sql.Parameters, parameterNamePrefix).Results.Select.ToArray();
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

        private static void MapExpressions(ITypedSqlLinq local, SQLinq<TResult> newItem)
        {
            if (local.Expressions.Any())
            {
                if (newItem.JoinExpressions.Any())
                {
                    RemapExpressionsFromJoins(local, newItem);
                }
                else
                {
                    newItem.Expressions.AddRange(local.Expressions);
                }
            }
        }

        private static void RemapExpressionsFromJoins(ITypedSqlLinq local, SQLinq<TResult> newItem)
        {
            var joinParameters = newItem.JoinExpressions
                                        .Select(x => ((LambdaExpression) x.OuterKeySelector))
                                        .SelectMany(x => x.Parameters);

            foreach (var expression in local.Expressions)
            {
                var localExpression = expression;

                var expParams = ((LambdaExpression) localExpression).Parameters;

                foreach (var param in expParams)
                {
                    //re-write existing expressions from the root to the new named join object if needed.
                    var match = joinParameters.FirstOrDefault(x => x.Type == param.Type && x.Name != param.Name);
                    if (match != null)
                    {
                        var newParam = Expression.Parameter(param.Type, match.Name);
                        var newExpression = new PredicateRewriterVisitor(newParam).Visit(localExpression);
                        newItem.Expressions.Add(newExpression);
                    }
                    else
                    {
                        //no matchng types, so just carry on
                        newItem.Expressions.Add(localExpression);
                    }
                }
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