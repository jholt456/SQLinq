//Copyright (c) Chris Pietschmann 2014 (http://pietschsoft.com)
//Licensed under the GNU Library General Public License (LGPL)
//License can be found here: http://sqlinq.codeplex.com/license

using SQLinq.Compiler;
using SQLinq.Dynamic;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

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
    public class SQLinq<T> : ITypedSqlLinq, IQueryable<T>, IQueryProvider,IOrderedQueryable<T>
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
            this.GroupExpressions = new List<ISQLinqGrouping>();
            this.Dialect = dialect;

            expression = Expression.Constant(this);
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
        public List<ISQLinqGrouping> GroupExpressions { get; private set; }

        public int? TakeRecords
        {
            get => takeRecords ?? this.Parent?.TakeRecords;
            private set => takeRecords = value;
        }

        public int? SkipRecords
        {
            get => skipRecords ?? this.Parent?.SkipRecords;
            private set => skipRecords = value;
        }

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
        /// <param name="selector"></param>
        /// <returns>The SQLinq instance to allow for method chaining.</returns>
        public SqlGroupBy<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> selector)
        {
            var result = new SqlGroupBy<T, TKey>(selector, this);

            this.GroupExpressions.Add(result);
            return result;

        }

        public SqlGroupBy<T, TKey, TElement> GroupBy<TKey, TElement>(Expression<Func<T, TKey>> keySelector, Expression<Func<T, TElement>> elementSelector)
        {
            var result = new SqlGroupBy<T, TKey, TElement>(keySelector, elementSelector, this);

            this.GroupExpressions.Add(result);
            return result;
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

        public virtual SQLinqJoin<TResult, T> Join<TInner, TKey, TResult>(
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
        protected static List<Expression> MapExpressions(List<Expression> expressions , List<ISQLinqTypedJoinExpression> joins)
        {
            var expressionResults = new List<Expression>();
            if (expressions.Any())
            {
                if (joins.Any())
                {
                    expressionResults.AddRange(RemapWhereExpressionsFromJoins(expressions, joins));
                }
                else
                {
                    expressionResults.AddRange(expressions);
                }
            }

            return expressionResults;
        }

        protected static List<Expression> RemapWhereExpressionsFromJoins(List<Expression> expressions, List<ISQLinqTypedJoinExpression> joins )
        {
            var expressionResults = new List<Expression>();
            var joinParameters = joins
                                        .Select(x => ((LambdaExpression)x.OuterKeySelector))
                                        .SelectMany(x => x.Parameters);

            foreach (var expression in expressions)
            {
                var localExpression = expression;
                IEnumerable<ParameterExpression> expParams = null;
                if (localExpression.NodeType == ExpressionType.Call)
                {
                    expParams = ((MethodCallExpression) localExpression).Arguments
                        .Where(x => x.NodeType == ExpressionType.Quote)
                        .Select(x => (UnaryExpression) x).Where(x=>x.Operand.NodeType  == ExpressionType.Lambda).SelectMany(x=>((LambdaExpression)x.Operand).Parameters);
                }
                else if(localExpression.NodeType == ExpressionType.Lambda )
                {
                    expParams = ((LambdaExpression) localExpression).Parameters;
                }
                else if (localExpression.NodeType == ExpressionType.MemberAccess)
                {
                    var parameterExpressions = ((MemberExpression)((MemberExpression)localExpression).Expression);
                    expParams = null;
                }

                if (expParams == null || !expParams.Any())
                {
                    continue;
                }

                foreach (var param in expParams)
                {
                    //re-write existing expressions from the root to the new named join object if needed.
                    var match = joinParameters.FirstOrDefault(x => x.Type == param.Type && x.Name != param.Name);
                    if (match != null)
                    {
                        var newParam = Expression.Parameter(param.Type, match.Name);
                        var newExpression = new PredicateRewriterVisitor(newParam).Visit(localExpression);
                        expressionResults.Add(newExpression);
                    }
                    else
                    {
                        //no matchng types, so just carry on
                        expressionResults.Add(localExpression);
                    }
                }
            }

            return expressionResults;
        }

        protected static Expression RemapSelectorFromGroups(Expression expression, List<ISQLinqGrouping> groups)
        {
            if (!groups.Any()) return expression;

            var joinParameters = groups
                .Select(x => ((LambdaExpression) x.GroupingExpression));

            if (joinParameters.Count() == 1 && groups.Count() == 1)
            {
                var body = ((LambdaExpression)expression).Body;
                var expressionType = body.NodeType;
                if (expressionType == ExpressionType.New)
                {

                    var selectorExp = (NewExpression)body;

                    var lambdaGroup = (LambdaExpression)groups[0].GroupingExpression;
                    var lambdaGroupType = lambdaGroup.Body.NodeType;
                    if (lambdaGroupType == ExpressionType.MemberAccess)
                    {
                        var resultExp = Expression.New(selectorExp.Constructor, new[] {lambdaGroup.Body}, selectorExp.Members);

                        var lambda = Expression.Lambda(resultExp, lambdaGroup.Parameters);
                        return lambda;
                    }
                    else if (lambdaGroupType == ExpressionType.New)
                    {
                        var selectorMembers = selectorExp.Members;
                        var selectorArguments = selectorExp.Arguments;
                        var groupingExpression = ((NewExpression) lambdaGroup.Body);
                        var groupingExpressionArguments = groupingExpression.Arguments;
                        var groupingExpressionMembers = groupingExpression.Members;

                        var finalArguments = new List<Expression>();

                        //remap the member access args from the grouping to the select
                        foreach (var memberInfo in selectorArguments)
                        {
                            MemberInfo match = groupingExpressionMembers.FirstOrDefault(x => memberInfo is MemberExpression && x.Name == ((MemberExpression) memberInfo).Member.Name);

                            if (match != null)
                            {
                                var idx = groupingExpressionMembers.IndexOf(match);
                                finalArguments.Add(groupingExpressionArguments[idx]);
                            }
                            else
                            {
                                finalArguments.Add(memberInfo);
                            }
                        }
                        var resultExp = Expression.New(selectorExp.Constructor, finalArguments, selectorMembers);

                        var lambda = Expression.Lambda(resultExp, lambdaGroup.Parameters);
                        return lambda;
                    }
                    else
                    {
                        throw new Exception($"Unsupported Group Expression Type ({lambdaGroupType})");
                    }
                }
                else if (expressionType == ExpressionType.Parameter)
                {
                    var lambdaGroup = (LambdaExpression)groups[0].GroupingExpression;

                    return lambdaGroup;
                }
                else if (expressionType == ExpressionType.Lambda)
                {
                    return RemapSelectorFromGroups(body, groups);
                }
            }
           

            return expression;
        }

        protected  IList<Tuple<Expression, bool>> RemapOrderByExpressionsFromGroups(IList<Tuple<Expression, bool>> expressions, List<ISQLinqGrouping> groups)
        {
            if (!groups.Any()) return expressions;
            if (!expressions.Any()) return expressions;

            var expressionResults = new List<Tuple<Expression, bool>>();
            var groupExpressions = groups.Select(x => ((LambdaExpression)x.GroupingExpression)).ToList();

            if (groupExpressions.Count() == 1 && groups.Count() == 1)
            {
                var exp = expressions[0];
                var body = ((LambdaExpression)exp.Item1).Body;
                var expressionType = body.NodeType;

                var groupingExpression = groupExpressions[0];
                if (expressionType == ExpressionType.Lambda)
                {
                    var result = ProcessOrderByExpression(groupingExpression, ((LambdaExpression) body).Body);
                    expressionResults.Add(Tuple.Create(result, exp.Item2));
                }
                else
                {
                    var result = ProcessOrderByExpression(groupingExpression, body);
                    expressionResults.Add(Tuple.Create(result, exp.Item2));
                }
                
            }

            return expressionResults;
        }

        public Expression ProcessOrderByExpression(Expression groupExpression, Expression orderBy)
        {

            var expressionType = orderBy.NodeType;
            var body = orderBy;
            var groupingExpression = groupExpression;
            if (expressionType == ExpressionType.New)
            {
                var selectorExp = (NewExpression)body;

                var lambdaGroup = (LambdaExpression)groupExpression;
                if (selectorExp.Members.Count == 1)
                {
                    var resultExp = Expression.New(selectorExp.Constructor, new[] { lambdaGroup.Body }, selectorExp.Members);

                    var lambda = Expression.Lambda(resultExp, lambdaGroup.Parameters);

                    return (Expression)lambda;
                }
                else
                {
                    var resultExp = Expression.New(selectorExp.Constructor, ((NewExpression)lambdaGroup.Body).Arguments, selectorExp.Members);

                    var lambda = Expression.Lambda(resultExp, lambdaGroup.Parameters);
                    return (Expression) lambda;
                }
            }
            else if (expressionType == ExpressionType.Parameter)
            {
                var lambdaGroup = ((LambdaExpression)groupingExpression);
                return (Expression) lambdaGroup;
            }
            else if (expressionType == ExpressionType.MemberAccess)
            {
                var exp = ((MemberExpression) orderBy).Expression;
                var lambdaGroup = ((LambdaExpression)groupingExpression);
                return orderBy;
            }

            throw new Exception($"Unsupported OrderBy Type ({expressionType})");
        }

        protected internal string GetTableName(bool withAs = false)
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
            var join = this.ToSql_Join(_parameterNumber, parameterNamePrefix, parameters);
            _parameterNumber = existingParameterCount + parameters.Count;

            // GroupBy
            var groupByResult = this.ToSQL_GroupBy(_parameterNumber, parameterNamePrefix, parameters, join);
            _parameterNumber = existingParameterCount + parameters.Count;

            //// SELECT
            var selectResult = this.ToSQL_Select(_parameterNumber, parameterNamePrefix, parameters, join, groupByResult);
            _parameterNumber = existingParameterCount + parameters.Count;

            // WHERE
            var whereResult = this.ToSQL_Where(_parameterNumber, parameterNamePrefix, parameters, join);
            _parameterNumber = existingParameterCount + parameters.Count;

           

            // ORDER BY
            var orderbyResult = this.ToSQL_OrderBy(_parameterNumber, parameterNamePrefix, parameters, join);
            _parameterNumber = existingParameterCount + parameters.Count;

            return new SQLinqSelectResult(this.Dialect)
            {
                Select = selectResult.Select.ToArray(),
                //Select = this.Expressions.Any() ? selectResult.Select.ToArray() : join.Any() ? join.SelectMany(x=>x.Results.Select).Distinct().ToArray() : selectResult.Select.ToArray(),
                Distinct = this.DistinctValue,
                Take = this.TakeRecords,
                Skip = this.SkipRecords,
                Table = tableName,
                Join = join.Select(x=>x.ToQuery()).ToArray(),
                Where = whereResult == null ? null : whereResult.SQL,
                OrderBy = orderbyResult.Select.ToArray(),
                GroupBy = groupByResult.Select.ToArray(),
                Parameters = parameters
            };
        }

        private SqlExpressionCompilerSelectorResult ToSQL_GroupBy(int parameterNumber, string parameterNamePrefix, Dictionary<string, object> parameters, List<SQLinqTypedJoinResult> joins)
        {
            var result = new SqlExpressionCompilerSelectorResult();

            var items = GetGroupingExpressionTree();

            for (var i = 0; i < items.Count; i++)
            {
                var r = SqlExpressionCompiler.CompileSelector(this.Dialect, parameterNumber, parameterNamePrefix, items[i], joins.Any(), false);
                foreach (var s in r.Select)
                {
                    result.Select.Add(s);
                }
                foreach (var p in r.Parameters)
                {
                    result.Parameters.Add(p.Key, p.Value);
                }
            }
            foreach (var item in result.Parameters)
            {
                parameters.Add(item.Key, item.Value);
            }

            return result;
        }

        private List<Expression> GetGroupingExpressionTree()
        {
            //hoist groups
            var parentItems = GetGroupingTree();

            var items = this.GroupExpressions.Select(x => x.GroupingExpression).ToList();

            if (parentItems != null)
            {
                items = parentItems.Select(x =>  x.GroupingExpression).ToList();
            }
            return items;
        }

        private IEnumerable<ISQLinqGrouping> GetGroupingTree()
        {
            ITypedSqlLinq current = this;
            IEnumerable<ISQLinqGrouping> parentItems = null;
            while (parentItems == null && current != null)
            {
                parentItems = (IEnumerable<ISQLinqGrouping>) current.GetType().GetProperty("GroupExpressions").GetValue(current, null);

                if (parentItems.Count() == 0)
                {
                    current = current.Parent;
                    parentItems = null;
                }
            }
            return parentItems ?? new List<ISQLinqGrouping>();
        }

        private List<SQLinqTypedJoinResult> ToSql_Join(int parameterNumber, string parameterNamePrefix, Dictionary<string, object> parameters)
        {
            var childJoins = GetJoinTree();

            if (!childJoins.Any())
            {
                return new List<SQLinqTypedJoinResult>();
            }

            var childExpressions = GetExpressionTree();
            //replace them
            var mappedExpressions = MapExpressions(childExpressions, childJoins);
            this.Expressions.Clear();
            this.Expressions.AddRange(mappedExpressions);

            var join = new List<SQLinqTypedJoinResult>();
            foreach (var j in childJoins)
            {
                join.Add(j.Process(parameters, parameterNamePrefix));
            }

            return join;
        }

        private List<ISQLinqTypedJoinExpression> GetJoinTree()
        {
            //hoist joins
            ITypedSqlLinq current = this;
            var childJoins = new List<ISQLinqTypedJoinExpression>();

            while (current != null)
            {
                childJoins.AddRange(current.JoinExpressions);
                current = current.Parent;
            }
            return childJoins;
        }

        private SqlExpressionCompilerResult ToSQL_Where(int parameterNumber, string parameterNamePrefix, IDictionary<string, object> parameters, IList<SQLinqTypedJoinResult> joins )
        {
            SqlExpressionCompilerResult whereResult = null;

            //if we have joins, the tree has already been crawled during remaping
            var childExpressions = joins.Any() ? this.Expressions : GetExpressionTree();

            if (childExpressions.Count > 0)
            {
                whereResult = SqlExpressionCompiler.Compile(this.Dialect, parameterNumber, parameterNamePrefix, childExpressions, joins.Any());
                foreach (var item in whereResult.Parameters)
                {
                    parameters.Add(item.Key, item.Value);
                }
            }
            return whereResult;
        }

        private List<Expression> GetExpressionTree()
        {
            //hoist wheres
            ITypedSqlLinq current = this;
            var childExpressions = new List<Expression>();

            while (current != null)
            {
                childExpressions.AddRange(current.Expressions);

                current = current.Parent;
            }
            return childExpressions;
        }

        private SqlExpressionCompilerSelectorResult ToSQL_Select(int parameterNumber, string parameterNamePrefix, IDictionary<string, object> parameters, IList<SQLinqTypedJoinResult> joins, SqlExpressionCompilerSelectorResult groupByResult)
        {
            //hoist selects
            ITypedSqlLinq current = this;
            Expression selector = null;
            while (selector == null && current != null)
            {
                selector = (Expression)current.GetType().GetProperty("Selector").GetValue(current, null);
                current = current.Parent;
            }

            var sqLinqGroupings = GetGroupingTree();

            selector = RemapSelectorFromGroups(selector, sqLinqGroupings.ToList());
            var selectResult = SqlExpressionCompiler.CompileSelector(this.Dialect, parameterNumber, parameterNamePrefix, selector, joins.Any());

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
                        var includeInSelect = true;
                        var attr = p.GetCustomAttributes(typeof(SQLinqColumnAttribute), true).FirstOrDefault() as SQLinqColumnAttribute;
                        if (attr != null)
                        {
                            includeInSelect = attr.Select;
                        }
                        if (includeInSelect)
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
                }
                else
                {
                    selectResult.Select.Add("*");
                }
            }
            return selectResult;
        }

        private SqlExpressionCompilerSelectorResult ToSQL_OrderBy(int parameterNumber, string parameterNamePrefix, IDictionary<string, object> parameters, IList<SQLinqTypedJoinResult> joins)
        {
            var orderbyResult = new SqlExpressionCompilerSelectorResult();

            var orderBys = GetOrderByTree();

            orderBys = RemapOrderByExpressionsFromGroups(orderBys.ToList(), GetGroupingTree().ToList()).ToList();

            orderBys = RemapWhereExpressionsFromJoins(orderBys.Select(x=>x.Item1).ToList(), GetJoinTree()).Select(x => Tuple.Create(x, true)).ToList();
            for (var i = 0; i < orderBys.Count; i++)
            {
                var r = SqlExpressionCompiler.CompileSelector(this.Dialect, parameterNumber, parameterNamePrefix, orderBys[i].Item1, joins.Any(), false);
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

        private List<Tuple<Expression, bool>> GetOrderByTree()
        {
            //hoist selects
            ITypedSqlLinq current = this;
            IEnumerable<dynamic> parentOrderBys = null;
            while (parentOrderBys == null && current != null)
            {
                parentOrderBys = (IEnumerable<dynamic>) current.GetType().GetProperty("OrderByExpressions").GetValue(current, null);

                if (parentOrderBys.Count() == 0)
                {
                    current = current.Parent;
                    parentOrderBys = null;
                }
            }

            var orderBys = this.OrderByExpressions.Select(o => Tuple.Create((Expression) o.Expression, o.Ascending)).ToList();

            if (parentOrderBys != null)
            {
                orderBys = parentOrderBys.Select(o => Tuple.Create((Expression) o.GetType().GetProperty("Expression").GetValue(o), (bool) o.GetType().GetProperty("Ascending").GetValue(o))).Cast<Tuple<Expression, bool>>().ToList();
            }
            return orderBys;
        }

        private readonly Expression _expression = null;

        public SqlExpressionCompilerSelectorResult ProcessJoinExpression(Expression exp, string parameterNamePrefix, IDictionary<string, object> parameters)
        {
            return SqlExpressionCompiler.CompileSelector(this.Dialect, parameters.Count, parameterNamePrefix, exp, true);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return this.Provider.Execute<IEnumerator<T>>(_expression);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return (IEnumerator<dynamic>) this.GetEnumerator();
        }

        public Type ElementType
        {
            get { return typeof(T); }
        }

        private Expression expression;
        private int? takeRecords;
        private int? skipRecords;

        public Expression Expression
        {
            get
            {
                return expression;
            }
        }

        public IQueryProvider Provider
        {
            //get { return new SQLinqQueryableProvider(); }
            get { return this; }
        }


        public IQueryable CreateQuery(Expression expression)
        {
            return (IQueryable<dynamic>)this.CreateQuery<dynamic>(expression);
        }

        protected virtual SQLinq<TElement> New<TElement>()
        {
            return new SQLinq<TElement>(this.GetTableName(true), this.Dialect) { Parent = this };
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            if (expression.NodeType == ExpressionType.Call)
            {
                var exp = ((MethodCallExpression)expression);

                var method = exp.Method.Name.ToLower();

                if (method.Equals("where"))
                {
                    this.Where(expression);
                }
                else if (method.Equals("select"))
                {
                    if (this.Parent != null)
                    {
                        throw new Exception("SUB QUERY");
                    }
                    var result = New<TElement>();
                    var quote = ((UnaryExpression)exp.Arguments[1]);
                    var lambda = ((LambdaExpression)quote.Operand);
                    this.Selector =  Expression.Lambda<Func<T, object>>(lambda, lambda.Name, lambda.TailCall, lambda.Parameters);
                    return (IQueryable<TElement>) result;
                }
                else if (method.Equals("groupby"))
                {
                    if (exp.Arguments.Count == 2)
                    {
                        MethodInfo m = this.GetType().GetMethods().FirstOrDefault(x => x.Name == "GroupBy" && x.GetGenericArguments().Length == 1);

                        var quote2 = ((UnaryExpression)exp.Arguments[1]);
                        var lambda2 = ((LambdaExpression)quote2.Operand);

                        MethodInfo generic = m.MakeGenericMethod(lambda2.ReturnType);

                        var result = generic.Invoke(this, new object[] { lambda2 });

                        return (IQueryable<TElement>)result;
                    }
                    else if (exp.Arguments.Count == 3)
                    {
                        MethodInfo m = this.GetType().GetMethods().FirstOrDefault(x=>x.Name == "GroupBy" && x.GetGenericArguments().Length == 2);

                        var quote2 = ((UnaryExpression)exp.Arguments[1]);
                        var lambda2 = ((LambdaExpression)quote2.Operand);

                        var quote3 = ((UnaryExpression)exp.Arguments[2]);
                        var lambda3 = ((LambdaExpression)quote3.Operand);

                        MethodInfo generic = m.MakeGenericMethod(lambda2.ReturnType, lambda3.ReturnType);

                        var result = generic.Invoke(this, new object[] { lambda2,lambda3 });

                        return (IQueryable<TElement>)result;
                    }
                    else
                    {
                        throw new Exception("Unsupported group parameters");
                    }
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
                else if (method.Equals("distinct"))
                {
                    //var quote = ((UnaryExpression)exp.Arguments[1]);
                    //var lambda = ((LambdaExpression)quote.Operand);
                    //var ordered = Expression.Lambda<Func<T, object>>(lambda, lambda.Name, lambda.TailCall, lambda.Parameters);

                    this.Distinct(true);
                }
                else if (method.Equals("skip"))
                {
                    this.Skip((int)((ConstantExpression)exp.Arguments[1]).Value);
                }
                else if (method.Equals("take"))
                {
                    this.Take((int)((ConstantExpression)exp.Arguments[1]).Value);
                }
                else if (method.Equals("join"))
                {
                    MethodInfo m = this.GetType().GetMethod("Join");

                    var quote2 = ((UnaryExpression)exp.Arguments[2]);
                    var lambda2 = ((LambdaExpression)quote2.Operand);

                    var quote3 = ((UnaryExpression)exp.Arguments[3]);
                    var lambda3 = ((LambdaExpression)quote3.Operand);

                    var quote4 = ((UnaryExpression)exp.Arguments[4]);
                    var lambda4 = ((LambdaExpression)quote4.Operand);

                    var constantExpression = ((ConstantExpression)exp.Arguments[1]);

                    var innerType = constantExpression.Type;
                    var value = constantExpression.Value;

                    MethodInfo generic = m.MakeGenericMethod(innerType.GetGenericArguments()[0], lambda2.ReturnType, lambda4.ReturnType);
                    
                    var result = generic.Invoke(this,new object[]{ value, lambda2, lambda3, lambda4 });

                    return (IQueryable<TElement>) result;
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
            return this.Execute<IEnumerator<dynamic>>(expression);
        }

        public virtual TResult Execute<TResult>(Expression expression)
        {
            //just allows toList to be called without bombing

            var type = typeof(TResult).GetGenericArguments()[0];
            var listType = typeof(List<>).MakeGenericType(type);
            var list = (IEnumerable)Activator.CreateInstance(listType);
            return (TResult)list.GetEnumerator();
        }

        public bool IsOrdered()
        {
            return GetOrderByTree().Count > 0;
        }
    }

    public class SqlGroupBy<T, TKey, TElement> : SQLinq<IGrouping<TKey, TElement>>, ISQLinqGrouping
    {
        private Expression<Func<T, TElement>> elementSelector;

        public SqlGroupBy(Expression<Func<T, TKey>> selector, Expression<Func<T, TElement>> elementSelector, SQLinq<T> parent) : base(parent.GetTableName(true), parent.Dialect)
        {
            this.GroupingExpression = selector;
            this.Parent = parent;
        }

        public Expression GroupingExpression { get; }
    }

    public class SqlGroupBy<T, TKey> : SQLinq<IGrouping<TKey, T>>,  ISQLinqGrouping
    {

        public SqlGroupBy(LambdaExpression exp, SQLinq<T> parent) : base(parent.GetTableName(true), parent.Dialect)
        {
            this.GroupingExpression = exp;
            this.Parent = parent;
        }

        public Expression GroupingExpression { get; private set; }
    }

    public interface ISQLinqGrouping
    {
        Expression GroupingExpression { get;  }
    }

    public class SQLinqQueryableProvider : IQueryProvider
    {

        public IQueryable CreateQuery(Expression expression)
        {
            Type elementType = expression.Type;
            try
            {
                return (IQueryable)Activator.CreateInstance(typeof(SQLinq<>).MakeGenericType(elementType), new object[] { this, expression });
            }
            catch (System.Reflection.TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
           return (IQueryable<TElement>) new SQLinq<TElement>();
        }

        public object Execute(Expression expression)
        {
            return SQLinqContext.Execute(expression, false);
        }

        public TResult Execute<TResult>(Expression expression)
        {
            bool IsEnumerable = (typeof(TResult).Name == "IEnumerable`1");

            return (TResult)SQLinqContext.Execute(expression, IsEnumerable);
        }
    }

    internal class SQLinqContext
    {
        public static object Execute(Expression expression, bool isEnumerable)
        {
            // The expression must represent a query over the data source. 
            if (!IsQueryOverDataSource(expression))
                throw new ArgumentException("No query over the data source was specified.");

            return null;
        }

        private static bool IsQueryOverDataSource(Expression expression)
        {
            // If expression represents an unqueried IQueryable data source instance, 
            // expression is of type ConstantExpression, not MethodCallExpression. 
            return (expression is MethodCallExpression);
        }
    }

    internal static class TypeSystem
    {
        internal static Type GetElementType(Type seqType)
        {
            Type ienum = FindIEnumerable(seqType);
            if (ienum == null) return seqType;
            return ienum.GetGenericArguments()[0];
        }

        private static Type FindIEnumerable(Type seqType)
        {
            if (seqType == null || seqType == typeof(string))
                return null;

            if (seqType.IsArray)
                return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType());

            if (seqType.IsGenericType)
            {
                foreach (Type arg in seqType.GetGenericArguments())
                {
                    Type ienum = typeof(IEnumerable<>).MakeGenericType(arg);
                    if (ienum.IsAssignableFrom(seqType))
                    {
                        return ienum;
                    }
                }
            }

            Type[] ifaces = seqType.GetInterfaces();
            if (ifaces != null && ifaces.Length > 0)
            {
                foreach (Type iface in ifaces)
                {
                    Type ienum = FindIEnumerable(iface);
                    if (ienum != null) return ienum;
                }
            }

            if (seqType.BaseType != null && seqType.BaseType != typeof(object))
            {
                return FindIEnumerable(seqType.BaseType);
            }

            return null;
        }
    }
}
