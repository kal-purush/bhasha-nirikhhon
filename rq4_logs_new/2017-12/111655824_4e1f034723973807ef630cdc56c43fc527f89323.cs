using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using Autofac;
using FakeItEasy;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NUnit.Framework;

namespace TagsCloudVisualization.Tests
{
    [TestFixture]
    public class CloudCombiner_Should
    {
        private ContainerBuilder builder;
        private Point center;
        private ICloudConfiguration cloudConfiguration;
        [SetUp]
        public void SetUp()
        {
            builder = new ContainerBuilder();

            cloudConfiguration = A.Fake<ICloudConfiguration>();
            A.CallTo(() => cloudConfiguration.MinFontSize)
                .Returns(10);
            A.CallTo(() => cloudConfiguration.MaxFontSize)
                .Returns(30);
            builder.RegisterInstance(cloudConfiguration).As<ICloudConfiguration>();
            var textReader = A.Fake<ITextReader>();
            builder.RegisterInstance(textReader).As<ITextReader>();
            builder.RegisterType<CloudCombiner>().As<ICloudCombiner>();
            builder.RegisterType<ArchimedeanCircularCloudLayouter>().As<ICircularCloudLayouter>();
            builder.Register(x => new Point());
        }

        

        public ICloudCombiner GetCombiner()
        {
            var container = builder.Build();
            center = container.Resolve<Point>();
            return container.Resolve<ICloudCombiner>();
        }

        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void GetCloud_AllWordsFormACircle(int wordsCount)
        {
            A.CallTo(() => cloudConfiguration.NumberOfWordsInTheCloud)
                .Returns(wordsCount);
            var statistic = new List<TagStatistic>();
            for (var i = 0; i < wordsCount; i++)
                statistic.Add(new TagStatistic(i.ToString(),i));
            var statisticGenerator = A.Fake<ITagStatisticsGenerator>();
            A.CallTo(() => statisticGenerator.GetStatistics(A<IEnumerable<string>>.Ignored))
                .Returns(statistic);
            builder.RegisterInstance(statisticGenerator).As<ITagStatisticsGenerator>();

            var cloud = GetCombiner().GetCloud();
            AllWordsFormACircle(cloud);
        }

        [TestCase(10,10,20)]
        [TestCase(10,10,10)]
        [TestCase(100,10,20)]
        [TestCase(100,10,10)]
        [TestCase(100,10,15)]
        public void GetCloud_SingleValuedDisplayOfWordSizes(int wordsCount, int min, int max)
        {
            A.CallTo(() => cloudConfiguration.MinFontSize)
                .Returns(min);
            A.CallTo(() => cloudConfiguration.MaxFontSize)
                .Returns(max);
            A.CallTo(() => cloudConfiguration.NumberOfWordsInTheCloud)
                .Returns(wordsCount);
            var statistic = new List<TagStatistic>();
            for (var i = 0; i < wordsCount; i++)
                statistic.Add(new TagStatistic(i.ToString(), i));
            var statisticGenerator = A.Fake<ITagStatisticsGenerator>();
            A.CallTo(() => statisticGenerator.GetStatistics(A<IEnumerable<string>>.Ignored))
                .Returns(statistic);
            builder.RegisterInstance(statisticGenerator).As<ITagStatisticsGenerator>();

            var cloud = GetCombiner().GetCloud();

            cloud.Words.Count().Should().Be(wordsCount);
            cloud.Words.Min(x => x.FontSize).Should().Be(min);
            cloud.Words.Max(x => x.FontSize).Should().Be(max);
        }

        private void AllWordsFormACircle(Cloud cloud)
        {
            var rectangles = cloud.Words.Select(x => x.Area).ToList();

            var totalArea = rectangles.Select(x => x.Width * x.Height).Sum();

            var currentCircularRadius = 1.3 * Math.Sqrt(totalArea / Math.PI);

            rectangles.Select(ToBorderPoints).SelectMany(x => x).Distinct()
                .Select(x => new Point(center.X - x.X, center.Y - x.Y))
                .Where(x => x.X * x.X + x.Y * x.Y > currentCircularRadius * currentCircularRadius)
                .Should()
                .HaveCount(0);
        }

        public static IEnumerable<Point> ToBorderPoints(Rectangle rect)
        {
            var x = rect.Location.X;
            var y = rect.Location.Y;
            var w = rect.Width;
            var h = rect.Height;
            return new[]
            {
                new Point(x,y),
                new Point(x + w, y + h),
                new Point(x + w, y),
                new Point(x, y + h),
            };
        }
    }
}