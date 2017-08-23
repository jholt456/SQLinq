using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace SQLinq.Compiler
{
    public abstract class MethodCallHandler
    {
        public abstract bool CanHandle(MethodCallExpression methodCallExpression);

        public abstract string Handle(ISqlDialect dialect, Expression rootExpression, MethodCallExpression methodCallExpression, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired);
    }

    public class OpenAccessSqlHandler : MethodCallHandler
    {
        public override bool CanHandle(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression.Method.DeclaringType.Name == "ExtensionMethods")
            {
                return true;
            }

            return false;
        }

        public override string Handle(ISqlDialect dialect, Expression rootExpression, MethodCallExpression methodCallExpression, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired)
        {
            var newArrayExpression = (NewArrayExpression)methodCallExpression.Arguments[1];
            var expression = ((UnaryExpression)newArrayExpression.Expressions[0]).Operand;
            var memberAccess = ((MemberExpression) expression);

            var col = SqlExpressionCompiler.GetMemberColumnName(memberAccess.Member, dialect);
            return string.Format(((ConstantExpression) methodCallExpression.Arguments[0]).Value.ToString(), col) ;
        }
    }

    public class EnumerableCountSqlHandler : MethodCallHandler
    {
        public override bool CanHandle(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression.Method.DeclaringType.Name == "Enumerable" && methodCallExpression.Method.Name == "Count")
            {
                return true;
            }

            return false;
        }

        public override string Handle(ISqlDialect dialect, Expression rootExpression, MethodCallExpression methodCallExpression, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired)
        {
            return "Count(*)";
        }
    }

    public class NullableHandler : MethodCallHandler
    {
        public override bool CanHandle(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression.Method.Name == nameof(Nullable<int>.GetValueOrDefault))
            {
                return true;
            }

            return false;
        }

        public override string Handle(ISqlDialect dialect, Expression rootExpression, MethodCallExpression methodCallExpression, IDictionary<string, object> parameters, Func<string> getParameterName, bool aliasRequired)
        {
            var memberAccess = ((MemberExpression)methodCallExpression.Object);

            var col = SqlExpressionCompiler.GetMemberColumnName(memberAccess.Member, dialect);
            var type = methodCallExpression.Method.ReturnType;

            return dialect.IsNull(col, GetDefault(type), parameters);
        }

        public static object GetDefault(Type type)
        {
            if (type.IsValueType)
            {
                return Activator.CreateInstance(type);
            }
            return null;
        }
    }
}