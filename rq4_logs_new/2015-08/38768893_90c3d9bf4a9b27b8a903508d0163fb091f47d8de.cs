using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Threading;
using NUnit.Framework;

using UrbanAirSharp.Dto;
using UrbanAirSharp.Response;

namespace UrbanAirSharp.Tests.Ignores
{
	[Explicit]
	//[TestFixture("a1e4ae13-7595-454c-bf96-e85b38ab265b")]
	[TestFixture]
	public class ChannelTagsCRUD
	{
		readonly UrbanAirSharpGateway _ua;
		readonly TagAudience _ta;
		readonly List<string> _remlist = new List<string>();

		public ChannelTagsCRUD() : this("a1e4ae13-7595-454c-bf96-e85b38ab265b") { }

		public ChannelTagsCRUD(string iosChannel)
		{
			CollectionAssert.IsNotEmpty(iosChannel);
			_ua = new UrbanAirSharpGateway();
			_ta = new TagAudience { IosChannel = iosChannel };

			_remlist.Add(Guid.NewGuid().ToString());
			_remlist.Add(Guid.NewGuid().ToString());
		}

		const string TAG_GROUP = "unit_test";

		[TestFixtureSetUp]
		public void T00_Clean()
		{
			var resp = _ua.Channel(_ta.IosChannel);
			OpTest(resp);
			Assert.IsNotNull(resp.Channel);
			if(resp.Channel.TagGroups != null && resp.Channel.TagGroups.ContainsKey(TAG_GROUP))
			{
				var op = new TagSet(_ta);
				op.Set.Add(TAG_GROUP, new string[0]);
				OpTest(_ua.ChannelTags(op));
			}
		}

		[Test]
		public void T01_Create()
		{
			var to = new TagAddRemove(_ta);
			to.Add.Add(TAG_GROUP, new[] { _remlist.First() });
			OpTest(_ua.ChannelTags(to));
		}

		void OpTest(BaseResponse resp)
		{
			Assert.IsNotNull(resp);
			Assert.IsTrue(resp.Ok, "{0} {1} | {2} {3}", resp.ErrorCode, resp.Error, resp.HttpResponseCode, resp.Message);
		}

		[Test]
		public void T02_Load()
		{
			Thread.Sleep(5000);
			ChannelResponse cr = _ua.Channel(_ta.IosChannel);
			GetTest(cr, () => new[] { _remlist.First() });
        }

		void GetTest(ChannelResponse cr, Func<ICollection<string>> getTags)
		{
			Assert.IsNotNull(cr);
			Assert.IsTrue(cr.Ok, "{0} {1} | {2} {3}", cr.ErrorCode, cr.Error, cr.HttpResponseCode, cr.Message);
			Assert.IsNotNull(cr.Channel);
			StringAssert.AreEqualIgnoringCase(_ta.IosChannel, cr.Channel.ChannelId);
			CollectionAssert.IsNotEmpty(cr.Channel.TagGroups);
			CollectionAssert.Contains(cr.Channel.TagGroups.Keys, TAG_GROUP);

			ICollection<string> tags = cr.Channel.TagGroups[TAG_GROUP];
			CollectionAssert.IsNotEmpty(tags);

			ICollection<string> reloads = getTags();
			foreach (string t in tags)
			{
				CollectionAssert.Contains(reloads, t);
			}
		}

		[Test]
		public void T03_Update()
		{
			var to = new TagSet(_ta);
			to.Set.Add(TAG_GROUP, _remlist);
			OpTest(_ua.ChannelTags(to));
		}

		[Test]
		public void T04_Reload()
		{
			Thread.Sleep(5000);
			ChannelResponse cr = _ua.Channel(_ta.IosChannel);
			GetTest(cr, () => _remlist);
		}

		[Test]
		public void T05_Delete()
		{
			var to = new TagAddRemove(_ta);
			to.Remove.Add(TAG_GROUP, _remlist);
			OpTest(_ua.ChannelTags(to));

			Thread.Sleep(5000);
			ChannelResponse cr = _ua.Channel(_ta.IosChannel);
			OpTest(cr);
			Assert.IsNotNull(cr.Channel);
			Assert.That(!cr.Channel.TagGroups.ContainsKey(TAG_GROUP) || cr.Channel.TagGroups[TAG_GROUP].Count == 0);
		}

	}
}