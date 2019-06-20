using FakeItEasy;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SQLinq.Dapper.Test
{
    [TestClass]
    public partial class ConnectionQueryableTests
    {

        [TestMethod]
        public void ConnectionQuery_001()
        {
            var mockConnection = A.Fake<IDbConnection>();
            IQueryable<Person> queryable = new ConnectionQueryable<Person>(new SqlServerDialect(), () => mockConnection);
            var query = queryable.Where(x => x.FirstName == "sdfsdf").OrderBy(x => x.FirstName).Skip(5).Take(5);

            //A.CallTo(() => mockConnection.Query(null, null, null, null, false, null,null)).WithAnyArguments().Invokes((a) =>
            //                                                                                        {
            //                                                                                           var args =  a.Arguments;
            //                                                                                        });
            var items = query.ToList();

            Assert.AreEqual(2, items.Count());
        }

        [TestMethod]
        public void ConnectionQuery_002()
        {
            var mockConnection = A.Fake<IDbConnection>();
            IQueryable<Person> queryable = new ConnectionQueryable<Person>(new SqlServerDialect(), () => mockConnection);
            var item = queryable.Where(x => x.FirstName == "sdfsdf").OrderBy(x => x.FirstName).Skip(5).Take(5).FirstOrDefault();

            //A.CallTo(() => mockConnection.Query(null, null, null, null, false, null,null)).WithAnyArguments().Invokes((a) =>
            //                                                                                        {
            //                                                                                           var args =  a.Arguments;
            //                                                                                        });
            //var items = query.ToList();

            Assert.AreEqual(item, null);
        }

        [TestMethod]
        public void ConnectionQuery_Contains_001()
        {
            var mockConnection = A.Fake<IDbConnection>();
            IQueryable<Child> queryable = new ConnectionQueryable<Child>(new SqlServerDialect(), () => mockConnection);
            var test = (new int[] { 1, 2, 3 }).ToList();
            queryable = queryable.Where(x => test.Contains(x.Height));

            var sqLinqResult = ((ITypedSqlLinq)queryable).ToSQL();
            var result = ((SQLinqSelectResult)sqLinqResult);

            Assert.AreEqual("[Height] IN @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<int>)result.Parameters.ElementAt(0).Value));
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);


            var queryResult = queryable.ToList();
            Assert.IsTrue(true);
        }

        [TestMethod]
        public void ConnectionQuery_Contains_002()
        {
            var mockConnection = A.Fake<IDbConnection>();
            IQueryable<Child> queryable = new ConnectionQueryable<Child>(new SqlServerDialect(), () => mockConnection);
            var test = (new int[] { 1, 2, 3 }).ToList();
            queryable = queryable.Where(InSet(queryable, x => x.Height, test));



            var sqLinqResult = ((ITypedSqlLinq)queryable).ToSQL();
            var result = ((SQLinqSelectResult)sqLinqResult);

            Assert.AreEqual("[Height] IN @sqlinq_1", result.Where);
            Assert.AreEqual(1, result.Parameters.Count);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<int>)result.Parameters.ElementAt(0).Value));
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);


            var queryResult = queryable.ToList();
            Assert.IsTrue(true);
        }


        [TestMethod]
        public void ConnectionQuery_Contains_003()
        {
            var mockConnection = A.Fake<IDbConnection>();
            IQueryable<Child> queryable = new ConnectionQueryable<Child>(new SqlServerDialect(), () => mockConnection);
            var test = (new int[] { 1, 2, 3 }).ToList();
            queryable = queryable.Where(InSet(queryable, x => x.HeightNullable.GetValueOrDefault(), test));



            var sqLinqResult = ((ITypedSqlLinq)queryable).ToSQL();
            var result = ((SQLinqSelectResult)sqLinqResult);


            Assert.AreEqual("ISNULL([HeightNullable],@defaultValue_1) IN @sqlinq_1", result.Where);
            Assert.AreEqual(2, result.Parameters.Count);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<int>)result.Parameters.ElementAt(0).Value));
            Assert.AreEqual(test, result.Parameters["@sqlinq_1"]);


            var queryResult = queryable.ToList();
            Assert.IsTrue(true);
        }



        public static Expression<Func<T, bool>> InSet<T, TValue>(IQueryable<T> source,
                                                                 System.Linq.Expressions.Expression<Func<T, TValue>>
                                                                     selector,
                                                                 IEnumerable<TValue> values)
        {
            MethodInfo method = null;
            foreach (MethodInfo tmp in typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static))
            {
                if (tmp.Name == nameof(Enumerable.Contains) && tmp.IsGenericMethodDefinition && tmp.GetParameters().Length == 2)
                {
                    method = tmp.MakeGenericMethod(typeof(TValue));
                    break;
                }
            }

            if (method == null)
                throw new InvalidOperationException("Unable to locate Contains");

            var keys = System.Linq.Expressions.Expression.Constant(values, typeof(IEnumerable<TValue>));
            var predicate = System.Linq.Expressions.Expression.Call(method, keys, selector.Body);
            return System.Linq.Expressions.Expression.Lambda<Func<T, bool>>(predicate, selector.Parameters[0]);
        }


        [TestMethod]
        public void ConnectionQuery_LIKE_001()
        {
            var name = "testName";
            var mockConnection = A.Fake<IDbConnection>();
            IQueryable<Person> queryable = new ConnectionQueryable<Person>(new SqlServerDialect(), () => mockConnection);
            queryable = queryable.Where(GenerateLike<Person>(x => x.FirstName, name, "StartsWith"));


            var result = ((ITypedSqlLinq)queryable).ToSQL();
            Assert.AreEqual("SELECT [ID], [FirstName], [LastName], [Age] FROM [Person] WHERE [FirstName] LIKE @sqlinq_1", result.ToQuery());
        }

        [TestMethod]
        public void ConnectionQuery_LIKE_002()
        {
            var name = "testName";
            var mockConnection = A.Fake<IDbConnection>();
            IQueryable<Person> queryable = new ConnectionQueryable<Person>(new SqlServerDialect(), () => mockConnection);
            Expression<Func<Person, bool>> exp = GenerateLike<Person>(x => x.FirstName, name, "StartsWith").Or(GenerateLike<Person>(x => x.LastName.Replace(" ", ""), name, "StartsWith"));
            queryable = queryable.Where(x => x.FirstName != null).Where(exp);


            var result = ((ITypedSqlLinq)queryable).ToSQL();
            Assert.AreEqual("SELECT [ID], [FirstName], [LastName], [Age] FROM [Person] WHERE [FirstName] IS NOT NULL AND ([FirstName] LIKE @sqlinq_1 OR REPLACE([LastName], ' ', @sqlinq_3) LIKE @sqlinq_4)", result.ToQuery());
        }

        public static Expression<Func<TSource, bool>> GenerateLike<TSource>(Expression<Func<TSource, string>> propExp,
                                                                          string right, string def = null)
        {
            var left = propExp.Body as Expression;

            //var left = test;

            right = right == null ? string.Empty : right.ToString();

            bool filterBeginning = right.StartsWith("%") || right.StartsWith("*");
            bool filterEnd = right.EndsWith("%") || right.EndsWith("*");

            MethodInfo methodInfo;

            var realValue = right.TrimStart('%').TrimEnd('%').TrimStart('*').TrimEnd('*').Trim();
            var newRightExpression = Expression.Constant(realValue, right.GetType());

            //var sourceProp = ExpressionExtensions.GetPropertyName(propExp);
            ParameterExpression sourceObject = propExp.Parameters[0];

            //var prop = left.Type.GetProperty(sourceProp);

            if (!filterBeginning && !filterEnd)
            {
                if (string.IsNullOrWhiteSpace(def))
                {
                    return Expression.Lambda<Func<TSource, bool>>(Expression.Equal(left, Expression.Constant(right)),
                        sourceObject);
                }

                methodInfo = left.Type.GetMethod(def, new Type[]
                                                      {
                                                          left.Type
                                                      });
            }
            else if (filterBeginning && filterEnd)
            {
                methodInfo = left.Type.GetMethod("Contains", new Type[]
                                                             {
                                                                 left.Type
                                                             });
                //return Expression.GreaterThan(Expression.Call(left, methodInfo, newRightExpression),
                //                                Expression.Constant(-1));
                //methodInfo = left.Type.GetMethod("IndexOf", new Type[] { left.Type });
                //return Expression.GreaterThan(Expression.Call(left, methodInfo, newRightExpression),
                //                                Expression.Constant(-1));
            }
            else if (filterBeginning)
            {
                methodInfo = left.Type.GetMethod("EndsWith", new Type[]
                                                             {
                                                                 left.Type
                                                             });
                //return Expression.Equal(Expression.Call(left, methodInfo, right),
                //                               Expression.Constant(true));
            }
            else
            {
                methodInfo = left.Type.GetMethod("StartsWith", new Type[]
                                                               {
                                                                   left.Type
                                                               });
            }

            Expression<Func<TSource, bool>> check = Expression.Lambda<Func<TSource, bool>>(
                Expression.Equal(Expression.Call(left, methodInfo, newRightExpression),
                    Expression.Constant(true)),
                sourceObject);

            return check;
            //return Expression.Equal(Expression.Call(left, methodInfo, newRightExpression),
            //    Expression.Constant(true));
        }
    }
}