using System.Linq;

namespace SQLinqTest
{
    public static class ProxyFilters
    {
        public static IQueryable<T> Apply<T>(IQueryable<T> baseQuery)
        {
            return baseQuery;
        }

        public static IQueryable<Person> WithName(this IQueryable<Person> people, string name)
        {
            people = people.Where(x => x.FirstName == name);
            return people;
        }
    }
}