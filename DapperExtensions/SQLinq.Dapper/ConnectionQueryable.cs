using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Dapper;

namespace SQLinq.Dapper
{
    public class ConnectionQueryable<T> : SQLinq<T>, IQueryable<T>
    {
        private readonly Func<IDbConnection> connectionFactory;
        private static readonly Type EnumerableType = typeof(IEnumerable);
        private static readonly Type IEnumerableType = typeof(IEnumerator);
        private static Type DapperType = typeof(global::Dapper.SqlMapper);
        private static readonly MethodInfo[] DapperMethods = DapperType.GetMethods();
        private static readonly Type CommandDefinitionType = typeof(CommandDefinition);
        private static readonly MethodInfo QueryMethod;

        static ConnectionQueryable()
        {
            QueryMethod = DapperMethods
                .First(x =>
                       {
                           var genericArguments = x.GetGenericArguments();
                           var parameterInfos = x.GetParameters();
                           return x.Name == $"{nameof(global::Dapper.SqlMapper.Query)}" && genericArguments.Length == 1 && parameterInfos.Length == 2 && parameterInfos[1].ParameterType == CommandDefinitionType;
                       });
        }

        public ConnectionQueryable(ISqlDialect dialect, Func<IDbConnection> connectionFactory) : base(dialect)
        {
            this.connectionFactory = connectionFactory;
        }

        public ConnectionQueryable(string getTableName, ISqlDialect dialect, Func<IDbConnection> connectionFactory) : base(getTableName, dialect)
        {
            this.connectionFactory = connectionFactory;
        }

        public override TResult Execute<TResult>(Expression expression)
        {
            using (var connection = connectionFactory())
            {
                var result = this.ToSQL();

                var sql = result.ToQuery();

                var parameters = new global::Dapper.DynamicParameters(result.Parameters);

                var type = typeof(TResult);

                if (type.IsGenericType && (EnumerableType.IsAssignableFrom(type) || IEnumerableType.IsAssignableFrom(type)))
                {
                    var genericArgument = type.GetGenericArguments()[0];
                    var conreteQuery = QueryMethod.MakeGenericMethod(genericArgument);

                    var commandDefinition = new CommandDefinition( sql, parameters, null, 0);

                    var results = (IEnumerable)conreteQuery.Invoke(null, new object[] { connection, commandDefinition });

                    if (IEnumerableType.IsAssignableFrom(type))
                    {
                        return (TResult)results.GetEnumerator();
                    }

                    return (TResult)results;
                }
                else if (expression != null && expression.NodeType == ExpressionType.Call)
                {
                    var call = (System.Linq.Expressions.MethodCallExpression)expression;

                    if (call.Method.Name == nameof(IList<TResult>.Count))
                    {
                        var query = this.Count();
                        var countQuery = query.ToSQL();
                        return connection.ExecuteScalar<TResult>(countQuery.ToQuery(), countQuery.Parameters);
                    }
                    else if (call.Method.Name == nameof(Queryable.FirstOrDefault))
                    {
                        var query = this.Take(1);
                        var countQuery = query.ToSQL();
                        return connection.Query<TResult>(countQuery.ToQuery(), countQuery.Parameters).FirstOrDefault();
                    }
                    else
                    {
                        throw new InvalidOperationException($"Unknown method call expression {call.Method.Name}");
                    }
                }
                else
                {
                    var results = connection.Query<TResult>(sql, parameters, null, true, null, CommandType.Text);

                    return results.FirstOrDefault();
                }

                //var results = connection.Query<TResult>(sql, parameters);

                //return (TResult) results;
                //while (results.MoveNext())
                //{
                //    yield return results.Current;
                //}
                // return results.Cast<dynamic>();
                //return base.Execute<TResult>(expression);
            }
        }

        public override SQLinqJoin<TResult, T> Join<TInner, TKey, TResult>(SQLinq<TInner> inner, Expression<Func<T, TKey>> outerKeySelector, Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<T, TInner, TResult>> resultSelector)
        {
            inner.Parent = this;

            var sqLinqJoinExpression = new SQLinqJoinExpression<T, TInner, TKey, TResult>(this.Dialect)
                                       {
                                           Outer = this,
                                           Inner = inner,
                                           OuterKeySelector = outerKeySelector,
                                           InnerKeySelector = innerKeySelector,
                                           ResultSelector = resultSelector
                                       };
            this.JoinExpressions.Add(sqLinqJoinExpression);

            return new ConnectionQueryableJoin<TResult, T>(this, connectionFactory) { Parent = this };
        }

        protected override SQLinq<TElement> New<TElement>()
        {
            var item = new ConnectionQueryable<TElement>(this.GetTableName(true), this.Dialect, connectionFactory) { Parent = this };
            return item;
        }
    }
}