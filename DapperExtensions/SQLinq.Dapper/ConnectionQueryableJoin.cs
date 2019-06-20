using System;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using Dapper;

namespace SQLinq.Dapper
{
    public class ConnectionQueryableJoin<T, TParent> : SQLinqJoin<T, TParent>, IQueryable<T>
    {
        private readonly Func<IDbConnection> connectionFactory;
        public ConnectionQueryableJoin(SQLinq<TParent> parent, Func<IDbConnection> connectionFactory) : base(parent)
        {
            this.connectionFactory = connectionFactory;
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
            //return new ConnectionQueryableJoin<TElement, T>(this, connectionFactory) { Parent = this };
            var item = new ConnectionQueryable<TElement>(this.GetTableName(true), this.Dialect, connectionFactory) { Parent = this };
            return item;
        }

        public override TElement Execute<TElement>(Expression expression)
        {
            using (var connection = connectionFactory())
            {
                var result = this.ToSQL();

                var sql = result.ToQuery();
                Console.WriteLine(sql);
                var parameters = new DynamicParameters(result.Parameters);

                var results = connection.Query<dynamic>(sql, parameters).GetEnumerator();

                return (TElement)results;
                //return base.Execute<TResult>(expression);
            }
        }

        public override ISQLinqResult ToSQL(int existingParameterCount = 0, string parameterNamePrefix = "sqlinq_")
        {
            return base.ToSQL(existingParameterCount, parameterNamePrefix);
        }
    }
}