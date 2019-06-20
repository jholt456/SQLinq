using Microsoft.VisualStudio.TestTools.UnitTesting;
using SQLinq.Compiler;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SQLinq;

namespace SQLinqTest.Compiler
{
    [TestClass]
    public class SqlExpressionCompilerTest
    {
        [TestMethod, ExpectedException(typeof(Exception))]
        public void CheckRequiredProperties_001()
        {
            var target = new SqlExpressionCompiler(parameterNamePrefix: string.Empty);
            target.Compile((Expression)null);            
        }

        [TestMethod, ExpectedException(typeof(Exception))]
        public void CheckRequiredProperties_002()
        {
            var target = new SqlExpressionCompiler(parameterNamePrefix: " ");
            target.Compile((Expression)null);
        }

        [TestMethod, ExpectedException(typeof(Exception))]
        public void CheckRequiredProperties_003()
        {
            var target = new SqlExpressionCompiler(parameterNamePrefix: null);
            target.Compile((Expression)null);
        }

        [TestMethod]
        public void Expression_Equals()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var searchVal = 12;
            Expression<Func<Person, bool>> exp = p=>p.Age == searchVal;
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, new [] { exp} , false);

            Assert.AreEqual($"[{nameof(Person.Age)}] = {paramName}", result.SQL);
            Assert.AreEqual(result.Parameters[$"{paramName}"], searchVal);
        }

        [TestMethod]
        public void Expression_GTE()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var searchVal = 12;
            Expression<Func<Person, bool>> exp = p => p.Age >= searchVal;
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, new[] { exp }, false);

            Assert.AreEqual($"[{nameof(Person.Age)}] >= {paramName}", result.SQL);
            Assert.AreEqual(result.Parameters[$"{paramName}"], searchVal);
        }

        [TestMethod]
        public void Expression_GTE_Nullable()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var searchVal = 12;
            Expression<Func<Child, bool>> exp = p => p.Height >= searchVal;
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, new[] { exp }, false);

            Assert.AreEqual($"[{nameof(Child.Height)}] >= {paramName}", result.SQL);
            Assert.AreEqual(result.Parameters[$"{paramName}"], searchVal);
        }

        [TestMethod]
        public void Expression_LTE()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var searchVal = 12;
            Expression<Func<Person, bool>> exp = p => p.Age <= searchVal;
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, new[] { exp }, false);

            Assert.AreEqual($"[{nameof(Person.Age)}] <= {paramName}", result.SQL);
            Assert.AreEqual(result.Parameters[$"{paramName}"], searchVal);
        }

        [TestMethod]
        public void Expression_LTE_Nullable()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var searchVal = 12;
            Expression<Func<Child, bool>> exp = p => p.Height <= searchVal;
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, new[] { exp }, false);

            Assert.AreEqual($"[{nameof(Child.Height)}] <= {paramName}", result.SQL);
            Assert.AreEqual(result.Parameters[$"{paramName}"], searchVal);
        }

        [TestMethod]
        public void Expression_Contains()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var test = (new int[] { 1, 2, 3 }).ToList();
           
            Expression<Func<Person, bool>> exp = p => test.Contains(p.Age);
            var expressions = new[] { exp };
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, expressions, false);

            Assert.AreEqual($"[{nameof(Person.Age)}] IN {paramName}", result.SQL);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<int>)result.Parameters.ElementAt(0).Value));
        }

        [TestMethod]
        public void Expression_Contains_Queryable()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var test = (new int[] { 1, 2, 3 }).ToList();
            var test2 = (new Person[] {}).ToList().AsQueryable();


            IQueryable<Person> exp = test2.Where(x=>test.Contains(x.Age));
            var expressions = new[] { exp.Expression};
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, expressions, false);

            Assert.AreEqual($"[{nameof(Person.Age)}] IN {paramName}", result.SQL);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<int>)result.Parameters.ElementAt(0).Value));
        }

        [TestMethod]
        public void Expression_Contains_Nullable()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var test = (new int[] { 1, 2, 3 }).ToList();

            Expression<Func<Child, bool>> exp = p => test.Contains(p.Height.Value);
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, new[] { exp }, false);

            Assert.AreEqual($"[{nameof(Child.Height)}] IN {paramName}", result.SQL);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<int>)result.Parameters.ElementAt(0).Value));
        }

        [TestMethod]
        public void Expression_Contains_NOT()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var test = (new int[] { 1, 2, 3 }).ToList();

            Expression<Func<Person, bool>> exp = p => test.Contains(p.Age);
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, new[] { exp }, false);

            Assert.AreEqual($"[{nameof(Person.Age)}] NOT IN {paramName}", result.SQL);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<int>)result.Parameters.ElementAt(0).Value));
        }

        [TestMethod]
        public void Expression_Contains_Nullable_NOT()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var test = (new int[] { 1, 2, 3 }).ToList();

            Expression<Func<Child, bool>> exp = p => test.Contains(p.Height.Value);
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, new[] { exp }, false);

            Assert.AreEqual($"[{nameof(Child.Height)}] NOT IN {paramName}", result.SQL);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<int>)result.Parameters.ElementAt(0).Value));
        }

        [TestMethod]
        public void Guid_Expression_Contains()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var test = (new Guid[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() }).ToList();

            Expression<Func<Person, bool>> exp = p => test.Contains(p.ID);
            var expressions = new[] { exp };
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, expressions, false);

            Assert.AreEqual($"[{nameof(Person.ID)}] IN {paramName}", result.SQL);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<Guid>)result.Parameters.ElementAt(0).Value));
        }

        [TestMethod]
        public void Guid_Expression_Contains_Queryable()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var test = (new Guid[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() }).ToList();
            var test2 = (new Person[] { }).ToList().AsQueryable();


            IQueryable<Person> exp = test2.Where(x => test.Contains(x.ID));
            var expressions = new[] { exp.Expression };
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, expressions, false);

            Assert.AreEqual($"[{nameof(Person.ID)}] IN {paramName}", result.SQL);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<Guid>)result.Parameters.ElementAt(0).Value));
        }

        [TestMethod]
        public void Guid_Expression_Contains_Nullable()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var test = (new Guid[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() }).ToList();

            Expression<Func<Child, bool>> exp = p => test.Contains(p.ParentID.Value);
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, new[] { exp }, false);

            Assert.AreEqual($"[{nameof(Child.ParentID)}] IN {paramName}", result.SQL);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<Guid>)result.Parameters.ElementAt(0).Value));
        }

        [TestMethod]
        public void Guid_Expression_Contains_NOT()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";

            var test = (new Guid[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() }).ToList();

            Expression<Func<Person, bool>> exp = p => test.Contains(p.ID);
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, new[] { exp }, false);

            Assert.AreEqual($"[{nameof(Person.ID)}] NOT IN {paramName}", result.SQL);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<Guid>)result.Parameters.ElementAt(0).Value));
        }

        [TestMethod]
        public void Guid_Expression_Contains_Nullable_NOT()
        {
            var parameterNamePrefix = "test";
            var startingParamCount = 0;
            var paramName = $"@{parameterNamePrefix}{startingParamCount + 1}";
            var test = (new Guid[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() }).ToList();

            Expression<Func<Child, bool>> exp = p => test.Contains(p.ParentID.Value);
            var result = SqlExpressionCompiler.Compile(new SqlServerDialect(), startingParamCount, parameterNamePrefix, new[] { exp }, false);

            Assert.AreEqual($"[{nameof(Child.ParentID)}] NOT IN {paramName}", result.SQL);
            Assert.IsTrue(test.SequenceEqual((IEnumerable<Guid>)result.Parameters.ElementAt(0).Value));
        }
    }
}
