//Copyright (c) Chris Pietschmann 2014 (http://pietschsoft.com)
//Licensed under the GNU Library General Public License (LGPL)
//License can be found here: http://sqlinq.codeplex.com/license

using SQLinq.Compiler;
using SQLinq.Dynamic;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace SQLinq
{
    public static class SQLinq
    {
        /// <summary>
        /// Creates a new SQLinq object for the Type of the object specified.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj">The object that defines the Type to use for creating the SQLinq object instance for.</param>
        /// <returns></returns>
        public static SQLinq<T> Create<T>(T obj, string tableName, ISqlDialect dialect)
        {
            return new SQLinq<T>(tableName, dialect);
        }

        /// <summary>
        /// Creates a new SQLinq object for the Type of the object specified.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj">The object that defines the Type to use for creating the SQLinq object instance for.</param>
        /// <returns></returns>
        public static SQLinq<T> Create<T>(T obj, string tableName)
        {
            // initialize the Default ISqlDialect
            var dialect = DialectProvider.Create();
            return Create<T>(obj, tableName, dialect);
        }

        /// <summary>
        /// Creates a new DynamicSQLinq object for the specified table name.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static DynamicSQLinq Create(string tableName, ISqlDialect dialect)
        {
            return new DynamicSQLinq(dialect, tableName);
        }

        /// <summary>
        /// Creates a new DynamicSQLinq object for the specified table name.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static DynamicSQLinq Create(string tableName)
        {
            // initialize the Default ISqlDialect
            var dialect = DialectProvider.Create();
            return Create(tableName, dialect);
        }

        /// <summary>
        /// Creates a new SQLinqInsert object for the specified Object.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static SQLinqInsert<T> Insert<T>(T data, ISqlDialect dialect)
        {
            return new SQLinqInsert<T>(data, dialect);
        }

        /// <summary>
        /// Creates a new SQLinqInsert object for the specified Object.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static SQLinqInsert<T> Insert<T>(T data)
        {
            // initialize the Default ISqlDialect
            var dialect = DialectProvider.Create();
            return Insert<T>(data, dialect);
        }

        /// <summary>
        /// Creates a new SQLinqInsert object for the specified Object and table name.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static SQLinqInsert<T> Insert<T>(T data, string tableName, ISqlDialect dialect)
        {
            return new SQLinqInsert<T>(data, tableName, dialect);
        }

        /// <summary>
        /// Creates a new SQLinqInsert object for the specified Object and table name.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static SQLinqInsert<T> Insert<T>(T data, string tableName)
        {
            // initialize the Default ISqlDialect
            var dialect = DialectProvider.Create();
            return Insert<T>(data, tableName, dialect);
        }

        /// <summary>
        /// Creates a new SQLinqInsert object for the specified Object.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static SQLinqUpdate<T> Update<T>(T data, ISqlDialect dialect)
        {
            return new SQLinqUpdate<T>(data, dialect);
        }

        /// <summary>
        /// Creates a new SQLinqInsert object for the specified Object.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static SQLinqUpdate<T> Update<T>(T data)
        {
            // initialize the Default ISqlDialect
            var dialect = DialectProvider.Create();
            return Update<T>(data, dialect);
        }

        /// <summary>
        /// Creates a new SQLinqInsert object for the specified Object and table name.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static SQLinqUpdate<T> Update<T>(T data, string tableName, ISqlDialect dialect)
        {
            return new SQLinqUpdate<T>(data, tableName, dialect);
        }

        /// <summary>
        /// Creates a new SQLinqInsert object for the specified Object and table name.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static SQLinqUpdate<T> Update<T>(T data, string tableName)
        {
            // initialize the Default ISqlDialect
            var dialect = DialectProvider.Create();
            return Update<T>(data, tableName, dialect);
        }
    }

    /// <summary>
    /// Allows for Ad-Hoc SQL queries to be generated using LINQ in a stongly type manner, while also taking advantage of compile time validation.
    /// </summary>
    /// <typeparam name="T">The Type that contains a strongly typed reference of the scheme for the database table/view to be queried.</typeparam>
    public class SQLinq<T> : ITypedSqlLinq, IQueryable<T>, IQueryProvider, IOrderedQueryable<T>
    {
        /// <summary>
        /// Creates a new SQLinq object
        /// </summary>
        public SQLinq()
            : this(DialectProvider.Create())
        { }

        /// <summary>
        /// Creates a new SQLinq object
        /// </summary>
        public SQLinq(ISqlDialect dialect)
        {
            this.Expressions = new List<Expression>(); //= new List<Expression<Func<T, bool>>>();
            this.JoinExpressions = new List<ISQLinqTypedJoinExpression>();
            this.OrderByExpressions = new List<OrderByExpression>();
            this.Dialect = dialect;
        }

        /// <summary>
        /// Creates a new SQLinq object
        /// </summary>
        /// <param name="tableNameOverride">The database table name to use. This explicitly overrides any use of the SQLinqTable attribute.</param>
        public SQLinq(string tableNameOverride)
            : this(tableNameOverride, DialectProvider.Create())
        { }

        /// <summary>
        /// Creates a new SQLinq object
        /// </summary>
        /// <param name="tableNameOverride">The database table name to use. This explicitly overrides any use of the SQLinqTable attribute.</param>
        public SQLinq(string tableNameOverride, ISqlDialect dialect)
            : this(dialect)
        {
            this.TableNameOverride = tableNameOverride;
        }

        public string TableNameOverride { get; private set; }

        public ISqlDialect Dialect { get; private set; }

        //public List<Expression<Func<T, bool>>> Expressions { get; private set; }
        public List<Expression> Expressions { get; private set; }

        public Expression<Func<T, object>> Selector { get; protected set; }
        public List<ISQLinqTypedJoinExpression> JoinExpressions { get; private set; }
        public int? TakeRecords { get; private set; }
        public int? SkipRecords { get; private set; }
        public List<OrderByExpression> OrderByExpressions { get; private set; }

        private bool? DistinctValue { get; set; }

        public class OrderByExpression
        {
            public Expression<Func<T, object>> Expression { get; set; }
            public bool Ascending { get; set; }
        }

        public ITypedSqlLinq Parent { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="distinct">Boolean value indicating whether 'DISTINCT' rows should be returned from the generated SQL. Default is True</param>
        /// <returns>The SQLinq instance to allow for method chaining.</returns>
        public SQLinq<T> Distinct(bool distinct = true)
        {
            this.DistinctValue = distinct;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="take"></param>
        /// <returns>The SQLinq instance to allow for method chaining.</returns>
        public SQLinq<T> Take(int take)
        {
            this.TakeRecords = take;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="skip"></param>
        /// <returns>The SQLinq instance to allow for method chaining.</returns>
        public SQLinq<T> Skip(int skip)
        {
            this.Dialect.AssertSkip(this);

            this.SkipRecords = skip;
            return this;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <returns>A SQLinqCount instance that can be used for generating a Count query.</returns>
        public SQLinqCount<T> Count()
        {
            return new SQLinqCount<T>(this);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns>The SQLinq instance to allow for method chaining.</returns>
        public SQLinq<T> Where(Expression<Func<T, bool>> expression)
        {
            this.Expressions.Add(expression);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns>The SQLinq instance to allow for method chaining.</returns>
        public SQLinq<T> Where(Expression expression)
        {
            this.Expressions.Add(expression);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <returns>The SQLinq instance to allow for method chaining.</returns>
        public SQLinq<T> Select(Expression<Func<T, object>> selector)
        {
            this.Selector = selector;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="keySelector"></param>
        /// <returns>The SQLinq instance to allow for method chaining.</returns>
        public SQLinq<T> OrderBy(Expression<Func<T, object>> keySelector)
        {
            if (this.OrderByExpressions.Count > 0)
            {
                this.OrderByExpressions.Clear();
            }
            this.OrderByExpressions.Add(new OrderByExpression { Expression = keySelector, Ascending = true });
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="keySelector"></param>
        /// <returns>The SQLinq instance to allow for method chaining.</returns>
        public SQLinq<T> OrderByDescending(Expression<Func<T, object>> keySelector)
        {
            if (this.OrderByExpressions.Count > 0)
            {
                this.OrderByExpressions.Clear();
            }
            this.OrderByExpressions.Add(new OrderByExpression { Expression = keySelector, Ascending = false });
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="keySelector"></param>
        /// <returns>The SQLinq instance to allow for method chaining.</returns>
        public SQLinq<T> ThenBy(Expression<Func<T, object>> keySelector)
        {
            this.OrderByExpressions.Add(new OrderByExpression { Expression = keySelector, Ascending = true });
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="keySelector"></param>
        /// <returns>The SQLinq instance to allow for method chaining.</returns>
        public SQLinq<T> ThenByDescending(Expression<Func<T, object>> keySelector)
        {
            this.OrderByExpressions.Add(new OrderByExpression { Expression = keySelector, Ascending = false });
            return this;
        }

        public SQLinq<TResult> Join<TInner, TKey, TResult>(
            //SQLinq<TOuter> outer,
            SQLinq<TInner> inner,
            Expression<Func<T, TKey>> outerKeySelector,
            Expression<Func<TInner, TKey>> innerKeySelector,
            Expression<Func<T, TInner, TResult>> resultSelector)
        {
            // http://msdn.microsoft.com/en-us/library/bb738634(v=vs.90).aspx
            // http://byatool.com/c/linq-join-method-and-how-to-use-it/

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


            return new SQLinqJoin<TResult, T>(this);
        }


        internal string GetTableName(bool withAs = false)
        {
            var tableName = string.Empty;
            var tableAsName = string.Empty;

            if (!string.IsNullOrEmpty(this.TableNameOverride))
            {
                tableName = tableAsName = this.TableNameOverride;
            }
            else
            {
                // Get Table / View Name
                var type = typeof(T);
                tableName = type.Name;
                var tableAttribute = type.GetCustomAttributes(typeof(SQLinqTableAttribute), false).FirstOrDefault() as SQLinqTableAttribute;
                if (tableAttribute != null)
                {
                    // Table / View name is explicitly set, use that instead
                    tableName = tableAttribute.Table;
                }

               
                if (withAs)
                {
                    var joins = this.JoinExpressions;
                    if (joins.Count == 0)
                    {
                        if (this.Parent != null)
                        {
                            joins = this.Parent.JoinExpressions;
                        }
                    }
                    if (joins.Count > 0)
                    {
                        var je = joins[0];
                        ParameterExpression p = ((dynamic)je.OuterKeySelector).Parameters[0] as ParameterExpression;
                        if (p.Type != typeof(T))
                        {
                            p = ((dynamic)je.InnerKeySelector).Parameters[0] as ParameterExpression;
                        }

                        tableAsName = p.Name;
                    }
                }
            }

            if (tableAsName == tableName)
            {
                tableAsName = null;
            }
            tableName = this.Dialect.ParseTableName(tableName, tableAsName);

            //if (tableAsName != null)
            //{
            //    if (!tableAsName.StartsWith("["))
            //    {
            //        tableAsName = string.Format("[{0}]", tableAsName);
            //    }
            //}

            //if (tableName == tableAsName)
            //{
                return tableName;
            //}
            //else
            //{
            //    return string.Format("{0} AS {1}", tableName, tableAsName);
            //}
        }

        /// <summary>
        /// Returns a SQLinqResult that contains the information for the query.
        /// </summary>
        /// <param name="existingParameterCount">Used to set the unique id's of the query parameters. The first query parameter will be 'existingParameterCount' plus one.</param>
        /// <returns></returns>
        public virtual ISQLinqResult ToSQL(int existingParameterCount = 0, string parameterNamePrefix = SqlExpressionCompiler.DefaultParameterNamePrefix)
        {
            int _parameterNumber = existingParameterCount;

            var type = typeof(T);
            var parameters = new Dictionary<string, object>();

            // Get Table / View Name
            var tableName = this.GetTableName(true);
            var subqueryAttr = type.GetCustomAttributes(typeof(SQLinqSubQueryAttribute), false).FirstOrDefault() as SQLinqSubQueryAttribute;
            if (subqueryAttr != null)
            {
                var tableQuery = subqueryAttr.GetQuery(parameters);
                _parameterNumber = parameters.Count;
                tableName = string.Format("({0}) AS {1}", tableQuery, tableName);
            }

            //// JOIN
            var join = new List<SQLinqTypedJoinResult>();
            foreach (var j in this.JoinExpressions)
            {
                join.Add(j.Process(parameters, parameterNamePrefix));
            }

            //// SELECT
            var selectResult = this.ToSQL_Select(_parameterNumber, parameterNamePrefix, parameters);
            _parameterNumber = existingParameterCount + parameters.Count;

            // WHERE
            var whereResult = this.ToSQL_Where(_parameterNumber, parameterNamePrefix, parameters);
            _parameterNumber = existingParameterCount + parameters.Count;

            // ORDER BY
            var orderbyResult = this.ToSQL_OrderBy(_parameterNumber, parameterNamePrefix, parameters);
            _parameterNumber = existingParameterCount + parameters.Count;

            return new SQLinqSelectResult(this.Dialect)
            {
                Select = this.Expressions.Any() ? selectResult.Select.ToArray() : join.Any() ? join.SelectMany(x=>x.Results.Select).Distinct().ToArray() : selectResult.Select.ToArray(),
                Distinct = this.DistinctValue,
                Take = this.TakeRecords,
                Skip = this.SkipRecords,
                Table = tableName,
                Join = join.Select(x=>x.ToQuery()).ToArray(),
                Where = whereResult == null ? null : whereResult.SQL,
                OrderBy = orderbyResult.Select.ToArray(),
                Parameters = parameters
            };
        }

        private SqlExpressionCompilerResult ToSQL_Where(int parameterNumber, string parameterNamePrefix, IDictionary<string, object> parameters)
        {
            SqlExpressionCompilerResult whereResult = null;

            //hoist wheres
            ITypedSqlLinq current = this;
            var childExpressions = new List<Expression>();

            while (current != null)
            {
                childExpressions.AddRange(current.Expressions);

                current = current.Parent;
            }

            if (childExpressions.Count > 0)
            {
                whereResult = SqlExpressionCompiler.Compile(this.Dialect, parameterNumber, parameterNamePrefix, childExpressions, this.JoinExpressions.Any());
                foreach (var item in whereResult.Parameters)
                {
                    parameters.Add(item.Key, item.Value);
                }
            }
            return whereResult;
        }

        private SqlExpressionCompilerSelectorResult ToSQL_Select(int parameterNumber, string parameterNamePrefix, IDictionary<string, object> parameters)
        {
            //hoist selects
            ITypedSqlLinq current = this;
            Expression selector = null;
            while (selector == null && current != null)
            {
                selector = (Expression)current.GetType().GetProperty("Selector").GetValue(current, null);
                current = current.Parent;
            }

            var selectResult = SqlExpressionCompiler.CompileSelector(this.Dialect, parameterNumber, parameterNamePrefix, selector, this.JoinExpressions.Any());

           
            foreach (var item in selectResult.Parameters)
            {
                parameters.Add(item.Key, item.Value);
            }
            if (selectResult.Select.Count == 0)
            {
                var props = typeof(T).GetProperties();
                var usesSQLinqColumn = props.Where(d => d.GetCustomAttributes(typeof(SQLinqColumnAttribute), false).Length > 0).Count() > 0;
                if (usesSQLinqColumn)
                {
                    foreach (var p in props)
                    {
                        var selectName = SqlExpressionCompiler.GetMemberColumnName(p, this.Dialect);
                        var asName = this.Dialect.ParseColumnName(p.Name);
                        if (selectName == asName)
                        {
                            selectResult.Select.Add(selectName);
                        }
                        else
                        {
                            selectResult.Select.Add(string.Format("{0} AS {1}", selectName, asName));
                        }
                    }
                }
                else
                {
                    selectResult.Select.Add("*");
                }
            }
            return selectResult;
        }

        private SqlExpressionCompilerSelectorResult ToSQL_OrderBy(int parameterNumber, string parameterNamePrefix, IDictionary<string, object> parameters)
        {
            var orderbyResult = new SqlExpressionCompilerSelectorResult();

            //hoist selects
            ITypedSqlLinq current = this;
            ICollection parentOrderBys = null;
            while (parentOrderBys == null && current != null)
            {
                parentOrderBys = (ICollection)current.GetType().GetProperty("OrderByExpressions").GetValue(current, null);

                if (parentOrderBys.Count == 0)
                {
                    current = current.Parent;
                    parentOrderBys = null;
                }
            }


            var orderBys = this.OrderByExpressions.Select(o=>Tuple.Create((Expression)o.Expression, o.Ascending)).ToList();

            if (parentOrderBys != null)
            {
                orderBys = parentOrderBys.Cast<dynamic>().Select(o => Tuple.Create((Expression) o.Expression, o.Ascending)).Cast<Tuple<Expression, bool>>().ToList();
            }


            for (var i = 0; i < orderBys.Count; i++)
            {
                var r = SqlExpressionCompiler.CompileSelector(this.Dialect, parameterNumber, parameterNamePrefix, orderBys[i].Item1, this.JoinExpressions.Any());
                foreach (var s in r.Select)
                {
                    orderbyResult.Select.Add(s);
                }
                foreach (var p in r.Parameters)
                {
                    orderbyResult.Parameters.Add(p.Key, p.Value);
                }
            }
            foreach (var item in orderbyResult.Parameters)
            {
                parameters.Add(item.Key, item.Value);
            }
            for (var i = 0; i < orderBys.Count; i++)
            {
                if (!orderBys[i].Item2)
                {
                    orderbyResult.Select[i] = orderbyResult.Select[i] + " DESC";
                }
            }

            return orderbyResult;
        }

        private Expression _expression = null;

        public SqlExpressionCompilerSelectorResult ProcessJoinExpression(Expression exp, string parameterNamePrefix, IDictionary<string, object> parameters)
        {
            return SqlExpressionCompiler.CompileSelector(this.Dialect, parameters.Count, parameterNamePrefix, exp, true);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return (this as IQueryable).Provider.Execute<IEnumerator<T>>(_expression);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return (IEnumerator<T>) (this as IQueryable).GetEnumerator();
        }

        public Type ElementType
        {
            get { return typeof(T); }
        }

        public Expression Expression
        {
            get { return Expression.Constant(this); }
        }

        public IQueryProvider Provider
        {
            get { return this; }
        }

        public IQueryable CreateQuery(Expression expression)
        {
            return (IQueryable<T>)(this as IQueryProvider).CreateQuery<T>(expression);
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            if (expression.NodeType == ExpressionType.Call)
            {
                var exp = ((MethodCallExpression) expression);

                var method = exp.Method.Name.ToLower();

                if (method.Equals("where"))
                {
                    this.Where(expression);
                }
                else if (method.Equals("select"))
                {
                    var result = new SQLinq<TElement>(this.TableNameOverride, this.Dialect) { Parent = this };
                    var quote = ((UnaryExpression) exp.Arguments[1]);
                    var lambda = ((LambdaExpression) quote.Operand);
                    this.Selector = Expression.Lambda<Func<T, object>>(lambda, lambda.Name, lambda.TailCall, lambda.Parameters );
                    return result;
                }
                else if (method.Equals("orderby"))
                {
                    var quote = ((UnaryExpression)exp.Arguments[1]);
                    var lambda = ((LambdaExpression)quote.Operand);
                    var ordered = Expression.Lambda<Func<T, object>>(lambda, lambda.Name, lambda.TailCall, lambda.Parameters);

                    this.OrderBy(ordered);
                }
                else if (method.Equals("orderbydescending"))
                {
                    var quote = ((UnaryExpression)exp.Arguments[1]);
                    var lambda = ((LambdaExpression)quote.Operand);
                    var ordered = Expression.Lambda<Func<T, object>>(lambda, lambda.Name, lambda.TailCall, lambda.Parameters);

                    this.OrderByDescending(ordered);
                }
                else if (method.Equals("thenby"))
                {
                    var quote = ((UnaryExpression)exp.Arguments[1]);
                    var lambda = ((LambdaExpression)quote.Operand);
                    var ordered = Expression.Lambda<Func<T, object>>(lambda, lambda.Name, lambda.TailCall, lambda.Parameters);

                    this.ThenBy(ordered);
                }
                else if (method.Equals("thenbydescending"))
                {
                    var quote = ((UnaryExpression)exp.Arguments[1]);
                    var lambda = ((LambdaExpression)quote.Operand);
                    var ordered = Expression.Lambda<Func<T, object>>(lambda, lambda.Name, lambda.TailCall, lambda.Parameters);

                    this.ThenByDescending(ordered);
                }
            }
            else
            {
                if (!typeof(TElement).Equals(typeof(T)))
                {
                    throw new Exception("Only " + typeof(T).FullName + " objects are supported. The query expression is of type " + typeof(TElement).FullName);
                }
            }

            return (IQueryable<TElement>)this;
        }

        public object Execute(Expression expression)
        {
            return (this as IQueryProvider).Execute<IEnumerator<T>>(expression);
        }

        public TResult Execute<TResult>(Expression expression)
        {
            //just allows toList to be called without bombing

            var type = typeof(TResult).GetGenericArguments()[0];
            var listType = typeof(List<>).MakeGenericType(type);
            var list = (IEnumerable)Activator.CreateInstance(listType);
            return (TResult) list.GetEnumerator();
        }

        }
}
