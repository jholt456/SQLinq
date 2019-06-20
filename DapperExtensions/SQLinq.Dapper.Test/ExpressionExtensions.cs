using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SQLinq.Dapper.Test
{
  
        public static class ExpressionExtensions
        {
            public static Expression<Func<TInput, object>> ToUntypedExpression<TInput, TOutput>
                (this Expression<Func<TInput, TOutput>> expression)
            {
                // Add the boxing operation, but get a weakly typed expression
                Expression converted = Expression.Convert
                    (expression.Body, typeof(object));
                // Use Expression.Lambda to get back to strong typing
                return Expression.Lambda<Func<TInput, object>>
                    (converted, expression.Parameters);
            }

            public static Expression<Func<TInput, object>> ToUntypedExpression<TInput>
                (this LambdaExpression expression)
            {
                // Add the boxing operation, but get a weakly typed expression
                Expression converted = Expression.Convert
                    (expression.Body, typeof(object));
                // Use Expression.Lambda to get back to strong typing
                return Expression.Lambda<Func<TInput, object>>
                    (converted, expression.Parameters);
            }

            public static string GetMemeberName<T>(Expression<Func<T>> expr)
            {
                var lambda = expr as LambdaExpression;
                MemberExpression memberExpression;
                if (lambda.Body is UnaryExpression)
                {
                    var unaryExpression = lambda.Body as UnaryExpression;
                    memberExpression = unaryExpression.Operand as MemberExpression;
                }
                else
                {
                    memberExpression = lambda.Body as MemberExpression;
                }

                Debug.Assert(memberExpression != null,
                    "Please provide a lambda expression like 'n => n.PropertyName'");

                if (memberExpression != null)
                {
                    return memberExpression.Member.Name;
                }

                return null;
            }
            public static string GetPropertyName<TModel>(
                Expression<Func<TModel, object>> propertyExpression)
            {
                var lambda = propertyExpression as LambdaExpression;
                MemberExpression memberExpression;
                var body = lambda.Body as UnaryExpression;
                if (body != null)
                {
                    var unaryExpression = body;
                    memberExpression = unaryExpression.Operand as MemberExpression;
                }
                else
                {
                    memberExpression = lambda.Body as MemberExpression;
                }

                Debug.Assert(memberExpression != null,
                    "Please provide a lambda expression like 'n => n.PropertyName'");

                if (memberExpression != null)
                {
                    var propertyInfo = memberExpression.Member as PropertyInfo;

                    if (propertyInfo != null)
                    {
                        return propertyInfo.Name;
                    }
                }

                return null;
            }

            public static string GetPropertyName<TModel, TPropType>(
                Expression<Func<TModel, TPropType>> propertyExpression)
            {
                var lambda = propertyExpression as LambdaExpression;
                MemberExpression memberExpression;
                if (lambda.Body is UnaryExpression)
                {
                    var unaryExpression = lambda.Body as UnaryExpression;
                    memberExpression = unaryExpression.Operand as MemberExpression;
                }
                else
                {
                    memberExpression = lambda.Body as MemberExpression;
                }

                Debug.Assert(memberExpression != null,
                    "Please provide a lambda expression like 'n => n.PropertyName'");

                if (memberExpression != null)
                {
                    var propertyInfo = memberExpression.Member as PropertyInfo;

                    return propertyInfo.Name;
                }

                return null;
            }

            internal class SubstExpressionVisitor : System.Linq.Expressions.ExpressionVisitor
            {
                public Dictionary<Expression, Expression> subst = new Dictionary<Expression, Expression>();

                protected override Expression VisitParameter(ParameterExpression node)
                {
                    Expression newValue;
                    if (subst.TryGetValue(node, out newValue))
                    {
                        return newValue;
                    }
                    return node;
                }
            }

            //http://stackoverflow.com/questions/457316/combining-two-expressions-expressionfunct-bool about invoke piece
            public static Expression<Func<T, bool>> CombinePredicates<T>(
                this IList<Expression<Func<T, bool>>> predicateExpressions,
                IList<Func<Expression, Expression, BinaryExpression>> logicalFunctions)
            {
                Expression<Func<T, bool>> filter = null;

                if (predicateExpressions.Count > 0)
                {
                    Expression<Func<T, bool>> firstPredicate = predicateExpressions[0];
                    Expression body = firstPredicate.Body;
                    ParameterExpression param = firstPredicate.Parameters[0];

                    SubstExpressionVisitor visitor = new SubstExpressionVisitor();

                    for (int i = 1; i < predicateExpressions.Count; i++)
                    {
                        var currentPrameter = predicateExpressions[i].Parameters[0];

                        if (ReferenceEquals(currentPrameter, param))
                        {
                            //easy path no swapping needed
                            body = logicalFunctions[i - 1](body, predicateExpressions[i].Body);
                        }
                        else
                        {
                            //have to swap params
                            visitor.subst[currentPrameter] = param;
                            body = logicalFunctions[i - 1](body, visitor.Visit(predicateExpressions[i].Body));
                            //Expression.Invoke(predicateExpressions[i], param)); 
                        }
                    }
                    filter = Expression.Lambda<Func<T, bool>>(body, firstPredicate.Parameters);
                }

                return filter;
            }

            //http://stackoverflow.com/questions/457316/combining-two-expressions-expressionfunct-bool 
            private static Expression<Func<T, bool>> OrElse<T>(
                this Expression<Func<T, bool>> expr1,
                Expression<Func<T, bool>> expr2)
            {
                // need to detect whether they use the same
                // parameter instance; if not, they need fixing
                ParameterExpression param = expr1.Parameters[0];
                if (ReferenceEquals(param, expr2.Parameters[0]))
                {
                    // simple version
                    return Expression.Lambda<Func<T, bool>>(
                        Expression.OrElse(expr1.Body, expr2.Body), param);
                }
                // otherwise, keep expr1 "as is" and invoke expr2
                return Expression.Lambda<Func<T, bool>>(
                    Expression.OrElse(
                        expr1.Body,
                        Expression.Invoke(expr2, param)), param);
            }

            public static Expression<Func<T, bool>> AndAll<T>(
                this IEnumerable<Expression<Func<T, bool>>> predicateExpressions)
            {
                return predicateExpressions.CombinePredicates(Expression.AndAlso);
            }


            public static Expression<Func<T, bool>> And<T>(this Expression<Func<T, bool>> left, Expression<Func<T, bool>> right)
            {
                return (new List<Expression<Func<T, bool>>>() { left, right }).CombinePredicates(Expression.AndAlso);
            }

            public static Expression<Func<T, bool>> Or<T>(
                this Expression<Func<T, bool>> left, Expression<Func<T, bool>> right)
            {
                return (new List<Expression<Func<T, bool>>>() { left, right }).CombinePredicates(Expression.OrElse);
            }


            public static Expression<Func<T, bool>> OrAll<T>(
                this IEnumerable<Expression<Func<T, bool>>> predicateExpressions)
            {
                return predicateExpressions.CombinePredicates(Expression.OrElse);
            }
            public static Expression<Func<T, bool>> Not<T>(this Expression<Func<T, bool>> one)
            {
                var candidateExpr = one.Parameters[0];
                var body = Expression.Not(one.Body);

                return Expression.Lambda<Func<T, bool>>(body, candidateExpr);
            }

            public static Expression<Func<T, bool>> CombinePredicates<T>(
                this IEnumerable<Expression<Func<T, bool>>> predicateExpressions,
                Func<Expression, Expression, BinaryExpression> binary)
            {
                Expression<Func<T, bool>>[] filterExpressions = predicateExpressions.ToArray();

                if (filterExpressions.Any())
                {
                    Func<Expression, Expression, BinaryExpression>[] operators =
                        Enumerable.Repeat<Func<Expression, Expression, BinaryExpression>>(binary,
                                      filterExpressions.Length - 1)
                                  .ToArray();

                    return filterExpressions.CombinePredicates(operators);
                }

                return t => true;
            }
        }
    
}