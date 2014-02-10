using System;
using System.Diagnostics;
using ServiceStack.Redis;

namespace KVStore
{
	public static class Store
	{
	    private static readonly TimeSpan LockTimeout = TimeSpan.FromMilliseconds(1000);
        static readonly PooledRedisClientManager manager;
		static string _applicationName;
       

		static Store()
		{
			manager = new PooledRedisClientManager();
		}

		public static string ApplicationName
		{
			get
			{
				if (_applicationName == null)
					throw new Exception("Make sure to set Store.ApplicationName on startup");

				return _applicationName;
			}
			set { _applicationName = value; }
		}

		private static string AssembleKey(string key)
		{
			return string.Format ("{0}|{1}", ApplicationName, key);
		}

		public static T GetObject<T>(string key)
		{
            using (var redis = manager.GetClient())
            {
                var client = redis.As<T>();
                var theKey = AssembleKey(key);
				
				using (client.AcquireLock(LockTimeout)) 
				{
					return client.GetValue(theKey);
				}
			}
		}

		public static void SetObject<T>(string key, T theObject)
		{
			using (var redis = manager.GetClient()) 
			{
                var client = redis.As<T>();
				var theKey = AssembleKey(key);

                using (client.AcquireLock(LockTimeout)) 
				{
					client.SetEntry(theKey, theObject);
				}
			}
		}

		public static T TryGetValueOrRefresh<T>(string key, Func<T> func, TimeSpan tsTimeOut) where T :class
		{
			using (var redis = manager.GetClient())
			{
			    var client = redis.As<T>();
				var theKey = AssembleKey(key);

				using (client.AcquireLock(LockTimeout)) 
				{
					var theObject = client.GetValue(theKey);

					if (theObject == null) 
					{
						theObject = func();
					    if (theObject != null)
					    {
					        client.SetEntry(theKey, theObject, tsTimeOut);
					    }
					}

					return theObject;
				}
			}
		}
	}
}