using FDM90.Handlers;
using FDM90.Models;
using FDM90.Singleton;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.DataVisualization.Charting;
using System.Web.UI.WebControls;

namespace FDM90.Pages.Content
{
    public partial class Campaigns : System.Web.UI.Page
    {
        private List<Campaign> _userCampaigns;
        private ICampaignHandler _campaignHandler;
        private string[] metrics = { "Exposure", "Influence", "Engagement" };
        private static DataTable campaignDataTable;

        public Campaigns():this(new CampaignHandler())
        {

        }

        public Campaigns(ICampaignHandler campaignHandler)
        {
            _campaignHandler = campaignHandler;
        }

        protected void Page_Load(object sender, EventArgs e)
        {
            if (!Page.IsPostBack)
            {
                _userCampaigns = _campaignHandler.GetUserCampaigns(UserSingleton.Instance.CurrentUser.UserId);
                currentCampaignDropDown.DataSource = _userCampaigns.Select(s => s.CampaignName);
                currentCampaignDropDown.DataBind();

                if (!string.IsNullOrEmpty(Request.QueryString["CampaignName"]))
                    currentCampaignDropDown.SelectedIndex = _userCampaigns.FindIndex(campaign => campaign.CampaignName == Request.QueryString["CampaignName"]);

                metricDropDown.DataSource = metrics;
                metricDropDown.DataBind();

                UpdateCampaignDataTable();
            }
        }

        private void UpdateCampaignDataTable()
        {
            Campaign selectedCampaign = _userCampaigns.Where(x => x.CampaignName == currentCampaignDropDown.SelectedValue).First();

            if (_userCampaigns.Count() > 0 && !string.IsNullOrWhiteSpace(selectedCampaign.Progress))
            {
                campaignDataTable = new DataTable();
                campaignDataTable.Columns.Add("Source", typeof(string));
                campaignDataTable.Columns.Add("Week", typeof(string));
                campaignDataTable.Columns.Add("Metric", typeof(string));
                campaignDataTable.Columns.Add("Target", typeof(int));
                campaignDataTable.Columns.Add("Progress", typeof(int));
                campaignDataTable.Columns.Add("AccumulatedProgress", typeof(int));
                JObject progress = JObject.Parse(selectedCampaign.Progress);
                JObject target = JObject.Parse(selectedCampaign.Targets);

                foreach (JProperty media in progress.Children())
                {
                    foreach (JProperty week in media.Values().OrderBy(o => o.Path.Substring(4)))
                    {
                        foreach (JProperty metric in week.Values())
                        {
                            DataRow row = campaignDataTable.NewRow();
                            row[0] = media.Name;
                            row[1] = week.Name.Substring(4);
                            row[2] = metric.Name;
                            row[3] = target[media.Name][metric.Name];
                            row[4] = metric.Value;

                            if (campaignDataTable.AsEnumerable().Where(w => w[0].ToString() == media.Name && w[2].ToString() == metric.Name).Count() == 0)
                            {
                                row[5] = metric.Value;
                            }
                            else
                            {
                                row[5] = int.Parse(metric.Value.ToString())
                                            + int.Parse(campaignDataTable.AsEnumerable().Where(w => w[0].ToString() == media.Name && w[2].ToString() == metric.Name).Last()[5].ToString());
                            }

                            campaignDataTable.Rows.Add(row);
                        }
                    }
                }

                foreach (var groupRow in campaignDataTable.AsEnumerable().GroupBy(g => new { Week = g[1], Metric = g[2] }))
                {
                    DataRow row = campaignDataTable.NewRow();
                    row[0] = "Overall";
                    row[1] = groupRow.First()[1];
                    row[2] = groupRow.First()[2];

                    int accumulatedTarget = 0;

                    foreach(JProperty value in target.Children())
                    {
                        accumulatedTarget += int.Parse(value.Value[groupRow.First()[2]].ToString());
                    }

                    row[3] = accumulatedTarget;

                    row[4] = groupRow.Sum(x => int.Parse(x[4].ToString()));

                    if (campaignDataTable.AsEnumerable().Where(w => w[0].ToString() == "Overall" && w[2].ToString() == groupRow.First()[1].ToString()).Count() == 0)
                    {
                        row[5] = groupRow.Sum(x => int.Parse(x[5].ToString()));
                    }
                    else
                    {
                        row[5] = groupRow.Sum(x => int.Parse(x[5].ToString()))
                                    + int.Parse(campaignDataTable.AsEnumerable().Where(w => w[0].ToString() == "Overall" && w[2].ToString() == groupRow.First()[1].ToString()).Last()[5].ToString());
                    }

                    campaignDataTable.Rows.Add(row);

                }

                UpdateCampaigns();
            }
        }

        private void UpdateCampaigns()
        {
            if (campaignChart.Series.Any() || campaignChart.ChartAreas.Any() || campaignChart.Titles.Any())
            {
                campaignChart.Series.Clear();
                campaignChart.ChartAreas.Clear();
                campaignChart.Titles.Clear();
            }

            foreach (var mediaRows in campaignDataTable.AsEnumerable().Where(w => w[2].ToString() == metricDropDown.SelectedValue.ToString())
                                            .OrderBy(o => o[1]).GroupBy(x => x[0]))
            {
                Series progressSeries = new Series();
                Series limitSeries = new Series();
                ChartArea mediaChart = new ChartArea();

                mediaChart.Name = mediaRows.First()[0].ToString();
                mediaChart.AxisX.IsMarginVisible = false;
                mediaChart.AxisX.IsMarginVisible = false;
                mediaChart.AxisX.Title = campaignDataTable.Columns[1].ToString();
                mediaChart.AxisY.Minimum = double.Parse(mediaRows.First()[5].ToString());
                mediaChart.AxisY.Maximum = double.Parse(mediaRows.Last()[5].ToString()) > double.Parse(mediaRows.Last()[3].ToString())
                                                ? double.Parse(mediaRows.Last()[5].ToString()) + (double.Parse(mediaRows.Last()[5].ToString()) / 10) :
                                                double.Parse(mediaRows.Last()[3].ToString()) + (double.Parse(mediaRows.Last()[3].ToString()) / 10);
                progressSeries.Name = mediaRows.First()[0].ToString() + "Progress";
                progressSeries.ChartType = SeriesChartType.Line;
                progressSeries.Points.DataBind(mediaRows, campaignDataTable.Columns[1].ToString(), campaignDataTable.Columns[5].ToString(), null);

                limitSeries.Name = mediaRows.First()[0].ToString() + "Limit";
                limitSeries.ChartType = SeriesChartType.Line;
                limitSeries.Points.DataBind(mediaRows, campaignDataTable.Columns[1].ToString(), campaignDataTable.Columns[3].ToString(), null);

                if(mediaChart.Name == "Overall")
                {
                    campaignChart.ChartAreas.Insert(0, mediaChart);
                }
                else
                {
                    campaignChart.ChartAreas.Add(mediaChart);
                }

                campaignChart.Series.Add(progressSeries);
                campaignChart.Series.Add(limitSeries);

                foreach (Series series in campaignChart.Series.Where(s => s.Name.Contains(mediaRows.First()[0].ToString())))
                    series.ChartArea = mediaChart.Name;

                campaignChart.Titles.Add(mediaChart.Name);
                campaignChart.Titles[campaignChart.ChartAreas.Count() - 1].DockedToChartArea = mediaChart.Name;
                campaignChart.Titles[campaignChart.ChartAreas.Count() - 1].IsDockedInsideChartArea = false;
            }

            campaignChart.DataBind();
        }

        protected void metricDropDown_SelectedIndexChanged(object sender, EventArgs e)
        {
            UpdateCampaigns();
        }

        protected void currentCampaignDropDown_SelectedIndexChanged(object sender, EventArgs e)
        {
            UpdateCampaignDataTable();
        }
    }
}