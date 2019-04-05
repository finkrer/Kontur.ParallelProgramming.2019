using System;
using System.Collections.Generic;

namespace ClusterClient
{
    public static class Helpers
    {
        public static void Shuffle(this IList<int> list)
        {
            var n = list.Count;
            var random = new Random();
            for (var i = 0; i < n; i++)
            {
                var r = i + random.Next(n - i);
                var t = list[r];
                list[r] = list[i];
                list[i] = t;
            }
        }
    }
}