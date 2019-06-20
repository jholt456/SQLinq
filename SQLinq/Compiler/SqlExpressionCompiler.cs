//Copyright (c) Chris Pietschmann 2015 (http://pietschsoft.com)
//Licensed under the GNU Library General Public License (LGPL)
//License can be found here: http://sqlinq.codeplex.com/license

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SQLinq.Compiler
{
    public class SqlExpressionCompiler
    {
        public const string DefaultParameterNamePrefix = "sqlinq_";
        const string _NULL = "NULL";
        const string _Space = " ";

        public static readonly List<MethodCallHandler> CallHandlerRegistry = new List<MethodCallHandler> { new EnumerableCountSqlHandler(), new OpenAccessSqlHandler(), new NullableHandler()};

        public static SqlExpressionCompilerResult Compile(ISqlDialect dialect, int existingParameterCount, string parameterNamePrefix, IEnumerable<Expression> expressions, bool aliasRequired = false)
        {
            return new SqlExpressionCompiler(dialect, existingParameterCount, parameterNamePrefix).Compile(expressions, aliasRequired);
        }

        public static SqlExpressionCompilerSelectorResult CompileSelector(ISqlDialect dialect, int existingParameterCount, string parameterNamePrefix, Expression expression, bool aliasRequired = false, bool withAs = true)
        {
            return new SqlExpressionCompiler(dialect, existingParameterCount, parameterNamePrefix).CompileSelector(expression, aliasRequired, withAs);
        }

        public SqlExpressionCompiler(int existingParameterCount = 0, string parameterNamePrefix = DefaultParameterNamePrefix)
            : this(DialectProvider.Create(), existingParameterCount, parameterNamePrefix)
        { }

        public SqlExpressionCompiler(ISqlDialect dialect, int existingParameterCount = 0, string parameterNamePrefix = DefaultParameterNamePrefix)
        {
            this.ExistingParameterCount = existingParameterCount;
            this.ParameterNamePrefix = parameterNamePrefix;
            this.Dialect = dialect;
        }

        public ISqlDialect Dialect { get; private set; }

        public int ExistingParameterCount { get; set; }
        public string ParameterNamePrefix { get; set; }

        private void CheckRequiredProperties()
        {
            if (string.IsNullOrEmpty((this.ParameterNamePrefix ?? string.Empty).Trim()))
            {
                throw new Exception("SqlExpressionCompiler.ParameterNamePrefix is Null or Whitespace");
            }
        }

        #region Public Methods

        public SqlExpressionCompilerResult Compile(Expression expression, bool aliasRequired = false)
        {
            return this.Compile(new Expression[] { expression }, aliasRequired);
        }

        public SqlExpressionCompilerResult Compile(IEnumerable<Expression> expressions, bool aliasRequired = false)
        {
            this.CheckRequiredProperties();

            var result = new SqlExpressionCompilerResult();

            var sb = new StringBuilder();
            var isFirstWhere = true;
            foreach (var e in expressions)
            {
                if (!isFirstWhere)
                {
                    sb.Append(" AND ");
                }
                isFirstWhere = false;

                sb.Append(ProcessExpression(this.Dialect, e, e, result.Parameters, this.GetParameterName, aliasRequired));
            }
            result.SQL = sb.ToString();

            return result;
        }

        public SqlExpressionCompilerSelectorResult CompileSelector(Expression expression, bool aliasRequired = false, bool withAs = true)
        {
            return this.CompileSelector(new Expression[] { expression }, aliasRequired, withAs);
        }

        public SqlExpressionCompilerSelectorResult CompileSelector(IEnumerable<Expression> expressions, bool aliasRequired = false, bool withAs = true)
        {
            this.CheckRequiredProperties();

            var result = new SqlExpressionCompilerSelectorResult();

            foreach (var e in expressions)
            {
                ProcessSelector(this.Dialect, e, e, result.Select, result.Parameters, this.GetParameterName, aliasRequired, withAs);
            }

            return result;
        }

        #endregion

        #region Private Methods

        private string GetParameterName()
        {
            this.ExistingParameterCount++;
            return this.Dialect.ParameterPrefix + this.ParameterNamePrefix + this.ExistingParameterCount.ToString();
        }

        //private string ProcessExpression(Expression expression, IDictionary<string, object> parameters)
        //{
        //    return ProcessExpression(expression, expression, parameters, this.GetParameterName);
        //}

        #endregion

        #region Private Static Methods

        static void ProcessSelector(ISqlDialect dialect, Expression rootExpression, Expression e, IList<string> select, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired, bool withAs)
        {
            if (e == null)
            {
                //// Join
                //if (this.JoinExpressions.Count > 0)
                //{
                //    this.ProcessSelector(this.JoinExpressions[0].ResultSelector, select, parameters);
                //    return;
                //}

                // All
                //select.Add("*");
                return;
            }

            if (e.NodeType == ExpressionType.Lambda)
            {
                ProcessSelector(dialect, rootExpression, ((LambdaExpression)e).Body, select, parameters, getParameterName, aliasRequired, withAs);
            }
            else if (e.NodeType == ExpressionType.New)
            {
                var n = (NewExpression)e;
                var len = n.Members.Count();
                for (var i = 0; i < len; i++)
                {
                    var arg = n.Arguments[i];

                    if (arg.NodeType != ExpressionType.Parameter)
                    {
                        var field = ProcessExpression(dialect, rootExpression, arg, parameters, getParameterName, aliasRequired);
                        var alias = dialect.ParseColumnName(n.Members[i].Name);
                        if (field == alias || !withAs)
                        {
                            select.Add(field);
                        }
                        else
                        {
                            select.Add(string.Format("{0} AS {1}", field, alias));
                        }
                    }
                    else if (arg.NodeType == ExpressionType.Parameter)
                    {
                        var param = (ParameterExpression) arg;
                        var field = dialect.ParseColumnName(param.Name);
                        var alias = dialect.ParseColumnName(n.Members[0].Name);
                        if (field == alias || !withAs)
                        {
                            select.Add(field);
                        }
                        else 
                        {
                            select.Add($"{field} AS {alias}");
                        }
                    }
                }
            }
            else if (e.NodeType == ExpressionType.Convert)
            {
                var u = (UnaryExpression)e;
                select.Add(ProcessExpression(dialect, rootExpression, u.Operand, parameters, getParameterName, aliasRequired));
            }
            else if (e.NodeType == ExpressionType.MemberAccess)
            {
                var s = ProcessSingleSideExpression(dialect, rootExpression, e, parameters, getParameterName, aliasRequired);
                select.Add(s);
            }
            else if (e.NodeType == ExpressionType.Call)
            {
                var s = ProcessCallExpression(dialect, rootExpression, (MethodCallExpression)e, parameters, getParameterName, aliasRequired);
                select.Add(s);
            }
        }

        //http://weblogs.asp.net/mehfuzh/archive/2007/10/04/writing-custom-linq-provider.aspx
        static string ProcessExpression(ISqlDialect dialect, Expression rootExpression, Expression e, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired)
        {
            switch (e.NodeType)
            {
                case ExpressionType.Equal:
                    return ProcessEqualExpression(dialect, rootExpression, (BinaryExpression)e, parameters, getParameterName, aliasRequired);

                case ExpressionType.GreaterThan:
                    return ProcessGreaterThanExpression(dialect, rootExpression, (BinaryExpression)e, parameters, getParameterName, aliasRequired);

                case ExpressionType.GreaterThanOrEqual:
                    return ProcessGreaterThanOrEqualExpression(dialect, rootExpression, (BinaryExpression)e, parameters, getParameterName, aliasRequired);

                case ExpressionType.LessThan:
                    return ProcessLessThanExpression(dialect, rootExpression, (BinaryExpression)e, parameters, getParameterName, aliasRequired);

                case ExpressionType.LessThanOrEqual:
                    return ProcessLessThanOrEqualExpression(dialect, rootExpression, (BinaryExpression)e, parameters, getParameterName, aliasRequired);

                case ExpressionType.NotEqual:
                    //var nee = (BinaryExpression)e;
                    //return string.Format("({0} <> {1})", ProcessExpression(rootExpression, nee.Left, parameters, getParameterName), ProcessExpression(rootExpression, nee.Right, parameters, getParameterName));
                    return ProcessNotEqualExpression(dialect, rootExpression, (BinaryExpression)e, parameters, getParameterName, aliasRequired);

                case ExpressionType.And:
                case ExpressionType.AndAlso:
                    var aae = (BinaryExpression)e;
                    return string.Format("({0} AND {1})", ProcessExpression(dialect, rootExpression, aae.Left, parameters, getParameterName, aliasRequired), ProcessExpression(dialect, rootExpression, aae.Right, parameters, getParameterName, aliasRequired));
                    
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    var oee = (BinaryExpression)e;
                    return string.Format("({0} OR {1})", ProcessExpression(dialect, rootExpression, oee.Left, parameters, getParameterName, aliasRequired), ProcessExpression(dialect, rootExpression, oee.Right, parameters, getParameterName, aliasRequired));

                case ExpressionType.Add:
                    var ae = (BinaryExpression)e;
                    return string.Format("({0} + {1})", ProcessExpression(dialect, rootExpression, ae.Left, parameters, getParameterName, aliasRequired), ProcessExpression(dialect, rootExpression, ae.Right, parameters, getParameterName, aliasRequired));

                case ExpressionType.Subtract:
                    var se = (BinaryExpression)e;
                    return string.Format("({0} - {1})", ProcessExpression(dialect, rootExpression, se.Left, parameters, getParameterName, aliasRequired), ProcessExpression(dialect, rootExpression, se.Right, parameters, getParameterName, aliasRequired));

                case ExpressionType.Multiply:
                    var me = (BinaryExpression)e;
                    return string.Format("({0} * {1})", ProcessExpression(dialect, rootExpression, me.Left, parameters, getParameterName, aliasRequired), ProcessExpression(dialect, rootExpression, me.Right, parameters, getParameterName, aliasRequired));

                case ExpressionType.Divide:
                    var de = (BinaryExpression)e;
                    return string.Format("({0} / {1})", ProcessExpression(dialect, rootExpression, de.Left, parameters, getParameterName, aliasRequired), ProcessExpression(dialect, rootExpression, de.Right, parameters, getParameterName, aliasRequired));

                case ExpressionType.Modulo:
                    var moduloe = (BinaryExpression)e;
                    return string.Format("({0} % {1})", ProcessExpression(dialect, rootExpression, moduloe.Left, parameters, getParameterName, aliasRequired), ProcessExpression(dialect, rootExpression, moduloe.Right, parameters, getParameterName, aliasRequired));

                case ExpressionType.Lambda:
                    return ProcessExpression(dialect, rootExpression, ((LambdaExpression)e).Body, parameters, getParameterName, aliasRequired);

                case ExpressionType.Call:
                    return ProcessCallExpression(dialect, rootExpression, (MethodCallExpression)e, parameters, getParameterName, aliasRequired);

                case ExpressionType.Convert:
                    var u = (UnaryExpression)e;
                    return ProcessExpression(dialect, rootExpression, u.Operand, parameters, getParameterName, aliasRequired);

                default:
                    //if (e.NodeType == ExpressionType.MemberAccess)
                    //{
                    return ProcessSingleSideExpression(dialect, rootExpression, e, parameters, getParameterName, aliasRequired);
                //}

                //throw new Exception("Unrecognized NodeType (" + e.NodeType.ToString() + ")");
            }
        }

        static string ProcessCallExpression(ISqlDialect dialect, Expression rootExpression, MethodCallExpression e, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired)
        {
            var method = e.Method;
            string memberName = null;

            if (e.Object is ParameterExpression )
            {
                // This is used by DynamicSQLinqLambdaExpression
                memberName = "{FieldName}";
            }

            else if (e.Object is ConstantExpression)
            {
                throw new Exception("SqlExpressionCompiler.ProcessCallExpresion: ConstantExpression Unsupported");
            }
            else if (e.Object is MemberExpression && ((MemberExpression)e.Object).Expression.NodeType != ExpressionType.Constant)
            {
                var dyn = (dynamic) e.Object;
                var member = (MemberInfo)(dyn).Member;
                memberName = GetMemberColumnName(member, dialect);
                memberName = GetAliasedColumnName(dialect, dyn, memberName, aliasRequired);
            }
            else if (e.Object != null && e.GetType().IsGenericParameter)
            {
                // Get Column Name to Use
                var member = (MemberInfo)((dynamic)e.Object).Member;
                memberName = GetMemberColumnName(member, dialect);
            }
            else if (e.Object is MethodCallExpression)
            {
                var member = (MethodInfo) ((dynamic) e.Object).Method;

                var loclMethod = member;
                var localMember = GetMemberColumnName(((dynamic) e.Object).Object.Member, dialect);
                memberName = ReplaceMethodCall(dialect, rootExpression, ((dynamic) e.Object), parameters, getParameterName, loclMethod, localMember);
            }

            if (method.DeclaringType == typeof(Guid))
            {
                var parameterName = getParameterName();

                switch (method.Name.ToLower())
                {
                    case "newguid":
                        parameters.Add(parameterName, Guid.NewGuid());
                        return string.Format("{0}{1}", memberName, parameterName);

                    default:
                        throw new Exception("Unsupported Method Name (" + method.Name + ") on Guid object");
                }
            }
            else if (method.DeclaringType == typeof(string))
            {
                return ReplaceMethodCall(dialect, rootExpression, e, parameters, getParameterName, method, memberName);
            }
            else if ( (method.DeclaringType == typeof(Enumerable) || 
                        method.DeclaringType == typeof(Queryable)) &&
                        e.Arguments.Count == 2)
            {
                string parameterName = null;

                if (e.Arguments[1].NodeType == ExpressionType.MemberAccess)
                {
                    parameterName = GetExpressionValue(dialect, rootExpression, e.Arguments[0], parameters, getParameterName);
                    var dyn = (dynamic) e.Arguments[1];
                    var member = (MemberInfo) (dyn).Member;
                    memberName = GetMemberColumnName(member, dialect);
                    memberName = GetAliasedColumnName(dialect, dyn, memberName, aliasRequired);

                    return string.Format("{0} IN {1}", memberName, parameterName);
                }
                else if (e.Arguments[1].NodeType == ExpressionType.Quote)
                {
                    //this is used when building expressions from IQueryables
                    var result = ProcessExpression(dialect, rootExpression, ((UnaryExpression) e.Arguments[1]).Operand, parameters, getParameterName, aliasRequired);

                    return result;
                }
                else if (e.Arguments[1].NodeType == ExpressionType.Call)
                {
                    //this is used when building expressions from IQueryables
                    parameterName = GetExpressionValue(dialect, rootExpression, e.Arguments[0], parameters, getParameterName);
                    var dyn = (MethodCallExpression)e.Arguments[1];
                    var member = ProcessCallExpression(dialect, rootExpression, dyn, parameters, getParameterName, aliasRequired);
                  

                    return string.Format("{0} IN {1}", member, parameterName);

                    //return result;
                }
                else
                {
                    throw new Exception("Unsupported Queryble Node Type (" + e.Arguments[1].NodeType + ")");
                }
            }
            else if ((typeof(IEnumerable).IsAssignableFrom(method.DeclaringType) ||
                      typeof(ICollection).IsAssignableFrom(method.DeclaringType)) &&
                     e.Arguments.Count == 1 &&
                     e.Arguments[0].NodeType == ExpressionType.MemberAccess)
            {
                if (rootExpression.NodeType == ExpressionType.Lambda)
                {
                    var exp = ((LambdaExpression) rootExpression).Body;
                    var parameterName = GetExpressionValue(dialect, rootExpression, exp, parameters, getParameterName);

                    var dyn = (dynamic) e.Arguments[0];
                    var member = (MemberInfo) (dyn).Member;
                    memberName = GetMemberColumnName(member, dialect);
                    memberName = GetAliasedColumnName(dialect, dyn, memberName, aliasRequired);

                    return string.Format("{0} IN {1}", memberName, parameterName);
                }
                else if (rootExpression.NodeType == ExpressionType.Call)
                {
                    var exp = ((MethodCallExpression)rootExpression);

                    return ProcessCallExpression(dialect, exp.Arguments[1], exp, parameters, getParameterName, false);
                }
                else if (rootExpression.NodeType == ExpressionType.Quote)
                {
                    var exp = ((UnaryExpression)rootExpression);

                    return ProcessExpression(dialect, exp.Operand, exp.Operand, parameters, getParameterName, false);
                }
                else
                {
                    throw new Exception("Unsupported Call Node Type (" + e.NodeType + ")");
                }
            
            }
            else
            {
                var handler = CallHandlerRegistry.FirstOrDefault(x=>x.CanHandle(e));

                if (handler != null)
                {
                    return handler.Handle(dialect, rootExpression, e, parameters, getParameterName, aliasRequired);
                }
                else { 
                    throw new Exception("Unsupported Method Declaring Type (" + method.DeclaringType.Name + ")");
                }
            }
        }

        private static string ReplaceMethodCall(ISqlDialect dialect, Expression rootExpression, MethodCallExpression e, IDictionary<string, object> parameters, Func<string> getParameterName, MethodInfo method, string memberName)
        {
            string parameterName = null;
            string secondParameterName = null;
            if (e.Arguments.Count > 0)
            {
                parameterName = GetExpressionValue(dialect, rootExpression, e.Arguments[0], parameters, getParameterName);

                if (e.Arguments.Count > 1)
                {
                    secondParameterName = GetExpressionValue(dialect, rootExpression, e.Arguments[1], parameters, getParameterName);
                }
            }

            switch (method.Name.ToLower())
            {
                case "startswith":
                    parameters[parameterName] = parameters[parameterName].ToString() + "%";
                    return string.Format("{0} LIKE {1}", memberName, parameterName);

                case "endswith":
                    parameters[parameterName] = "%" + parameters[parameterName].ToString();
                    return string.Format("{0} LIKE {1}", memberName, parameterName);

                case "contains":
                    parameters[parameterName] = "%" + parameters[parameterName].ToString() + "%";
                    return string.Format("{0} LIKE {1}", memberName, parameterName);

                case "toupper":
                    return string.Format("UCASE({0})", memberName);

                case "tolower":
                    return string.Format("LCASE({0})", memberName);

                case "replace":
                    return string.Format("REPLACE({0}, {1}, {2})", memberName, parameterName, secondParameterName);

                case "substring":
                    if (secondParameterName != null)
                    {
                        return string.Format("SUBSTR({0}, {1}, {2})", memberName, parameterName, secondParameterName);
                    }
                    else
                    {
                        return string.Format("SUBSTR({0}, {1})", memberName, parameterName);
                    }

                case "indexof":
                    return string.Format("CHARINDEX({0}, {1})", parameterName, memberName);
                case "trim":
                    return string.Format("LTRIM(RTRIM({0}))", memberName);

                default:
                    throw new Exception("Unsupported Method Name (" + method.Name + ") on String object");
            }
        }

        static string ProcessBinaryExpression(ISqlDialect dialect, string sqlOperator, Expression rootExpression, BinaryExpression e, IDictionary<string, object> parameters, Func<string> getParameterName,bool aliasRequired)
        {
            var left = ProcessSingleSideExpression(dialect, rootExpression, e.Left, parameters, getParameterName, aliasRequired);
            var right = ProcessSingleSideExpression(dialect, rootExpression, e.Right, parameters, getParameterName, aliasRequired);
            
            var op = sqlOperator;
            if (right == _NULL)
            {
                if (sqlOperator == "=")
                {
                    op = "IS";
                }
                else if (sqlOperator == "<>")
                {
                    op = "IS NOT";
                }
            }
            else if (left.IndexOf(" LIKE ", StringComparison.OrdinalIgnoreCase) > -1)
            {
                return string.Format("{0}", left);
            }

            return string.Format("{1} {0} {2}", op, left, right);
        }

        static string ProcessLessThanOrEqualExpression(ISqlDialect dialect, Expression rootExpression, BinaryExpression e, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired)
        {
            return ProcessBinaryExpression(dialect, "<=", rootExpression, e, parameters, getParameterName, aliasRequired);
        }

        static string ProcessLessThanExpression(ISqlDialect dialect, Expression rootExpression, BinaryExpression e, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired)
        {
            return ProcessBinaryExpression(dialect, "<", rootExpression, e, parameters, getParameterName, aliasRequired);
        }

        static string ProcessGreaterThanOrEqualExpression(ISqlDialect dialect, Expression rootExpression, BinaryExpression e, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired)
        {
            return ProcessBinaryExpression(dialect, ">=", rootExpression, e, parameters, getParameterName, aliasRequired);
        }

        static string ProcessGreaterThanExpression(ISqlDialect dialect, Expression rootExpression, BinaryExpression e, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired)
        {
            return ProcessBinaryExpression(dialect, ">", rootExpression, e, parameters, getParameterName, aliasRequired);
        }

        static string ProcessEqualExpression(ISqlDialect dialect, Expression rootExpression, BinaryExpression e, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired)
        {
            return ProcessBinaryExpression(dialect, "=", rootExpression, e, parameters, getParameterName, aliasRequired);
        }

        static string ProcessNotEqualExpression(ISqlDialect dialect, Expression rootExpression, BinaryExpression e, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired)
        {
            return ProcessBinaryExpression(dialect, "<>", rootExpression, e, parameters, getParameterName, aliasRequired);
        }

        internal static string GetMemberColumnName(MemberInfo p, ISqlDialect dialect)
        {
            string retVal = p.Name;

            // Get Column Name to Use
            var columnAttribute = p.GetCustomAttributes(typeof(SQLinqColumnAttribute), false).FirstOrDefault() as SQLinqColumnAttribute;
            if (columnAttribute != null)
            {
                if (!string.IsNullOrEmpty(columnAttribute.Column))
                {
                    // Column name explicitly set, use that instead
                    retVal = columnAttribute.Column;
                }
            }

            return dialect.ParseColumnName(retVal);
        }

        /// <summary>
        /// This checks if the specified Expression is (in the end) an operation of a Parameter of the Root Expression.
        /// </summary>
        /// <param name="rootExpression"></param>
        /// <param name="e"></param>
        /// <returns></returns>
        static bool IsPropertyExpressionRootParameter(Expression rootExpression, Expression e)
        {
            var retVal = false;
            if (rootExpression is LambdaExpression)
            {
                var lambda = rootExpression as LambdaExpression;

                var lastExpression = TraverseToLastExpression(e);

                if (lastExpression.NodeType == ExpressionType.Parameter)
                {
                    if (lastExpression is ParameterExpression)
                    {
                        var pe = lastExpression as ParameterExpression;

                        foreach (var p in lambda.Parameters)
                        {
                            if (p.Name == pe.Name && p.Type == pe.Type)
                            {
                                retVal = true;
                                break;
                            }
                        }
                    }
                }
            }
            return retVal;
        }

        /// <summary>
        /// Gets the next sub-Expression from the specified Expression recursively until the last one is retrieved.
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        static Expression TraverseToLastExpression(Expression e)
        {
            if (e is MemberExpression)
            {
                return TraverseToLastExpression((e as MemberExpression).Expression);
            }
            return e;
        }

        static string ProcessSingleSideExpression(ISqlDialect dialect, Expression rootExpression, Expression e, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired)
        {
            switch (e.NodeType)
            {
                case ExpressionType.Parameter:
                    // This is used by DynamicSQLinqLambdaExpression
                    return "{FieldName}";

                case ExpressionType.Constant:
                    var val = GetExpressionValue(dialect, rootExpression, e, parameters, getParameterName);
                    return val.ToString();

                case ExpressionType.MemberAccess:
                    var dynExpr = ((dynamic)e).Expression;
                    if (dynExpr is ConstantExpression)
                    {
                        return GetExpressionValue(dialect, rootExpression, e, parameters, getParameterName);
                    }
                    if (dynExpr is MethodCallExpression)
                    {
                        throw new Exception("SqlExpression.ProcessSingleSideExpression: MethodCallExpression Unsupported");
                        /* **** TODO ****
                        var mce = (MethodCallExpression)dynExpr;
                        if (mce.Object is ConstantExpression)
                        {
                            var paramName = ProcessExpression(rootExpression, mce.Object, parameters, getParameterName);
                            return paramName; // BROKEN - Hit when calling a method within lambda expression
                        }
                        return ProcessCallExpression(rootExpression, mce, parameters, getParameterName);
                        */
                    }
                    else
                    {

                        var d = (dynamic)e;

                        var nullableType = typeof(Nullable<>);
                        if (d.NodeType == ExpressionType.MemberAccess && d.Expression != null)
                        {
                            //catch cases when the user does something like .Value or .HasValue
                            var declaringType = d.Expression.Type;
                           
                            if (declaringType.IsGenericType && declaringType.GetGenericTypeDefinition() == nullableType)
                            {
                                d = (MemberExpression) d.Expression;
                            }
                        }

                        var isNullableMember = (d.Member is PropertyInfo && d.Member.PropertyType.IsGenericType && d.Member.PropertyType.GetGenericTypeDefinition() == nullableType) ||
                                               (d.Member is FieldInfo && d.Member.FieldType.IsGenericType && d.Member.FieldType.GetGenericTypeDefinition() == nullableType);

                        if (((d.NodeType == ExpressionType.MemberAccess && d.Expression == null) ||
                              (!IsPropertyExpressionRootParameter(rootExpression, e) && d.Expression.NodeType != ExpressionType.Parameter))
                          )
                        {
                            // A property of an object is being used as a query parameter
                            // This isn't the object that represents a column in the database
                            if (isNullableMember)
                            {
                                return GetExpressionValue(dialect, rootExpression, e, parameters, getParameterName);
                            }
                            else
                            {
                                return GetExpressionValue(dialect, rootExpression, e, parameters, getParameterName);
                            }
                        }


                        if (isNullableMember)
                        {
                            var memberName = GetMemberColumnName(d.Member, dialect);
                            memberName = GetAliasedColumnName(dialect, d, memberName, aliasRequired);


                            var method = ((MemberExpression) e).Member.Name;

                            switch (method.ToLower())
                            {
                                case "hasvalue":
                                    memberName = $"{memberName} IS NOT NULL";
                                    break;
                                default:
                                    memberName = $"{memberName}";
                                    break;
                            }

                            return memberName;
                        }
                        else
                        {
                            // //////// Get Column Name to Use
                            var memberName = GetMemberColumnName(d.Member, dialect);
                            memberName = GetAliasedColumnName(dialect, d, memberName, aliasRequired);
                            return memberName;
                        }
                    }

                case ExpressionType.Multiply:
                case ExpressionType.Add:
                case ExpressionType.Subtract:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                    return ProcessExpression(dialect, rootExpression, e, parameters, getParameterName, aliasRequired);

                case ExpressionType.And:
                case ExpressionType.Or:
                case ExpressionType.Not:
                    return ProcessExpression(dialect, rootExpression, e, parameters, getParameterName, aliasRequired);

                case ExpressionType.Call:
                    return ProcessCallExpression(dialect, rootExpression, (MethodCallExpression)e, parameters, getParameterName, aliasRequired);
                case ExpressionType.Quote:
                    return ProcessExpression(dialect, rootExpression, ((UnaryExpression)e).Operand, parameters, getParameterName, aliasRequired);
                case ExpressionType.Convert:
                    return ProcessExpression(dialect, rootExpression, (UnaryExpression)e, parameters, getParameterName, aliasRequired);

                default:
                    throw new Exception("Unrecognized NodeType (" + e.NodeType.ToString() + ")");
            }
        }

        private static string GetAliasedColumnName(ISqlDialect dialect, dynamic d, string memberName, bool aliasRequired)
        {
            string methodName = null;

            PropertyInfo pi = null;
            try
            {
                if (d.Expression is MemberExpression)
                {
                   pi = d.Expression.Member as PropertyInfo;
                }
                else
                {
                    pi = null;
                }
            }
            catch
            {
                pi = null;
            }
            if (pi != null)
            {
                if (pi.PropertyType == typeof(string))
                {
                    if (memberName.ToLower() == "[length]")
                    {
                        methodName = "LEN";
                        memberName = GetMemberColumnName(pi, dialect);
                    }
                }
            }
            else
            {
                memberName = string.Format("{0}", memberName);
            }

            // ////////////////////////////////////////////////////////////////

            var addTableAlias = aliasRequired;

            // //////  Build full SQL to get data for expression
            string fullMemberName;
            if (addTableAlias)
            {
                string tableName = "";
                if (d.Expression.NodeType == ExpressionType.MemberAccess)
                {
                    var memberExp = (MemberExpression)d.Expression;
                    if (memberExp.Type.IsGenericType && memberExp.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        tableName = dialect.ParseTableName(((ParameterExpression)memberExp.Expression).Name);
                    }
                    else
                    {
                        tableName = dialect.ParseTableName(((MemberExpression) d.Expression).Member.Name);
                    }
                }
                else if (d.Expression.NodeType == ExpressionType.Constant)
                {
                    tableName = memberName;
                }
                else
                {
                    // Get Table / View Name to Use
                    tableName = dialect.ParseTableName(d.Expression.Name as String);
                }

                fullMemberName = string.Format("{0}.{1}", tableName, memberName);
            }
            else
            {
                fullMemberName = string.Format("{0}", memberName);
            }

            // ////// Build full SQL statement for this expression
            if (methodName != null)
            {
                return string.Format("{0}({1})", methodName, fullMemberName);
            }
            else
            {
                return fullMemberName;
            }
        }

        static string GetExpressionValue(ISqlDialect dialect, Expression rootExpression, Expression e, IDictionary<string, object> parameters, Func<string> getParameterName)
        {
            var de = (dynamic)e;

            if (de.NodeType == ExpressionType.MemberAccess)
            {
                object val = null;

                var t = (Type)de.Type;
                var fieldName = de.Member.Name;
                if (t == typeof(DateTime))
                {
                    if (fieldName == "Now")
                    {
                        val = DateTime.Now;
                    }
                    else if (fieldName == "UtcNow")
                    {
                        val = DateTime.UtcNow;
                    }
                }
                if (val != null)
                {
                    var id = getParameterName();
                    parameters.Add(id, dialect.ConvertParameterValue(val));
                    return id;
                }
            }

            if (de.NodeType == ExpressionType.Call)
            {
                object val = null;

                var t = (Type)de.Type;
                var exp = ((MethodCallExpression) de);

                if (exp.Object != null)
                {
                    val = GetMemberAccessValue(rootExpression, exp.Object);
                    if (val != null)
                    {
                        var id = getParameterName();

                        parameters.Add(id, dialect.ConvertParameterValue(val));
                        return id;
                    }
                }
                else 
                {
                    throw new Exception("Unsupported call expression");
                }
            }

            var ce = (e is ConstantExpression) ? e : de.Expression;
            if (ce.NodeType == ExpressionType.Constant)
            {
                var val = ce.Value;
                if (!(e is ConstantExpression))
                {
                    var t = (Type)ce.Type;
                    var fieldName = de.Member.Name;

                    var fieldInfo = t.GetField(fieldName) ?? t.GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
                    if (fieldInfo != null)
                    {
                        val = fieldInfo.GetValue(val);
                    }
                    else
                    {
                        PropertyInfo propInfo = t.GetProperty(fieldName) ?? t.GetProperty(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
                        if (propInfo != null)
                        {
                            val = propInfo.GetValue(val, null);
                        }
                    }

                    var sqlLinq = val as ISQLinq;
                    var sqLinqResult = sqlLinq?.ToSQL(parameters.Count, "sub_query_params") as SQLinqSelectResult;

                    if (sqLinqResult != null)
                    {
                        val = $"({sqLinqResult.ToQuery()})";

                        foreach (var item in sqLinqResult.Parameters)
                        {
                            parameters.Add(item);
                        }

                        return val;
                    }

                    //var mt = (System.Reflection.MemberTypes)(de.Member).MemberType;
                    //if (mt == System.Reflection.MemberTypes.Field)
                    //{
                    //    var fieldInfo = t.GetField(fieldName) ?? t.GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
                    //    val = fieldInfo.GetValue(val);
                    //}
                    //else if (mt == System.Reflection.MemberTypes.Property)
                    //{
                    //    PropertyInfo propInfo = t.GetProperty(fieldName) ?? t.GetProperty(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
                    //    val = propInfo.GetValue(val, null);
                    //}
                }


                if (val == null)
                {
                    return _NULL;
                }
                else if ((val as string) == _Space)
                {
                    return "' '";
                }
                else
                {
                    var id = getParameterName();
                    parameters.Add(id, dialect.ConvertParameterValue(val));
                    return id;
                }
            }
            else if (ce.NodeType == ExpressionType.MemberAccess)
            {
                var val = GetMemberAccessValue(rootExpression, e);

                if (val == null)
                {
                    return _NULL;
                }
                else if ((val as string) == _Space)
                {
                    return "' '";
                }
                else
                {
                    var id = getParameterName();
                    parameters.Add(id, dialect.ConvertParameterValue(val));
                    return id;
                }
            }
            else if (ce.NodeType == ExpressionType.Parameter)
            {
                var fieldName = de.Member.Name;

                if (fieldName != null)
                {
                    var id = getParameterName();
                    parameters.Add(id, dialect.ConvertParameterValue(fieldName));
                    return id;
                }
            }
            return null;
        }

        static object GetMemberAccessValue(Expression rootExpression, Expression e)
        {
            var de = (dynamic)e;

            if (e.NodeType == ExpressionType.Parameter)
            {
                return null;
            }
            else if (e.NodeType == ExpressionType.Constant)
            {
                var ce = e as ConstantExpression;
                return ce.Value;
            }
            else if (e.NodeType == ExpressionType.MemberAccess)
            {
                var val = GetMemberAccessValue(rootExpression, de.Expression);
                //if (val == null) return null;

                if (de.Member is PropertyInfo)
                {
                    var memberPropInfo = de.Member as PropertyInfo;
                    return memberPropInfo.GetValue(val, null);
                }
                else if (de.Member is FieldInfo)
                {
                    var memberFieldInfo = de.Member as FieldInfo;
                    return memberFieldInfo.GetValue(val);
                }
                else
                {
                    throw new NotSupportedException("SQExpressionCompiler.GetMemberAccessValue: Member Type is not supported");
                }
            }

            throw new NotSupportedException("SQExpressionCompiler.GetMemberAccessValue: Expression Not Supported");
        }

        #endregion
    }
}
