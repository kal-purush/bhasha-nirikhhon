using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using IoT_Environment.Models;
using IoT_Environment.DTO;
using IoT_Environment.Filters;
using Microsoft.Extensions.Logging;
using IoT_Environment.Logging;

namespace IoT_Environment.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ReportsController : ControllerBase
    {
        private readonly IoTContext _context;
        private readonly ILogger<ReportsController> _logger;

        public ReportsController(IoTContext context, ILogger<ReportsController> logger)
        {
            _context = context;
            _logger = logger;
        }

        // GET: api/Reports
        [HttpGet]
        public async Task<ActionResult<IEnumerable<DataReport>>> GetReports([FromQuery] ReportFilters filters)
        {
            _logger.LogInformation(ApiEventIds.ReadAllReports, "Getting all Reports using {Filters}", filters);

            List<DataReport> results = await _context.Reports
                .Where(r => r.Posted > DateTime.UtcNow.AddMinutes(-(double)filters.LastMinutes))
                .Where(r => string.IsNullOrWhiteSpace(filters.DataType) ||
                       string.Equals(r.DataTypeNavigation.Name, filters.DataType, StringComparison.OrdinalIgnoreCase)) // could InvariantCultureIgnoreCase be better here?
                .OrderBy(r => r.Posted)
                .Select(r => new DataReport
                {
                    // null checks here?
                    DataType = r.DataTypeNavigation.Name,
                    DataUnits = r.DataTypeNavigation.Unit,
                    DeviceName = r.DeviceNavigation.Name,
                    DeviceType = r.DeviceNavigation.ConnectionType,
                    PostedOn = r.Posted,
                    RelayName = r.DeviceNavigation.RelayNavigation.Name,
                    Value = r.Value
                })
                .ToListAsync();

            _logger.LogInformation(ApiEventIds.ReadAllReports, "Found {Count} Reports", results.Count);

            return results;
        }

        // POST: api/Reports
        [HttpPost]
        public async Task<ActionResult<Report>> PostReport(DeviceData data)
        {
            _logger.LogInformation(ApiEventIds.CreateReport, "Began creating Report: {Data}", data);

            Relay relay = await _context.Relays.FirstOrDefaultAsync(r => r.PhysicalAddress == data.RelayPhysicalAddress);
            if (relay == null)
            {
                _logger.LogInformation(ApiEventIds.ReadRelayNotFound, "Failed creating Report -- could not find Relay: {Address}", data.RelayPhysicalAddress);
                NotFound($"Relay {data.RelayPhysicalAddress} not registered");
            }

            _logger.LogInformation(ApiEventIds.ReadRelay, "Found Relay information: Id {Relay}", relay.Id);

            Device device = await _context.Devices.FirstOrDefaultAsync(d => d.Address == data.DeviceAddress && d.RelayNavigation == relay);
            if (device == null)
            {
                _logger.LogInformation(ApiEventIds.ReadDeviceNotFound, "Failed creating Report -- could not find Device {Device} for Relay {Relay}", data.DeviceAddress, data.RelayPhysicalAddress);
                NotFound($"Device {data.DeviceAddress} for Relay {data.RelayPhysicalAddress} not found");
            }

            _logger.LogInformation(ApiEventIds.ReadRelay, "Found Device information: Id {Device}", device.Id);

            if (relay.NetworkAddress != data.RelayNetworkAddress)
            {
                _logger.LogInformation(ApiEventIds.UpdateRelay, "Updating Relay network address: {Old} -> {New}", relay.NetworkAddress, data.RelayNetworkAddress);
                relay.NetworkAddress = data.RelayNetworkAddress;
                _context.Entry(relay).State = EntityState.Modified;
            }

            DataType dataType = await _context.DataTypes.FindAsync(data.DataType);
            if (dataType == null)
            {
                _logger.LogInformation(ApiEventIds.CreateDataType, "Generating new Data Type: Id {DataType}", data.DataType);
                dataType = new() { Id = data.DataType };
                _context.DataTypes.Add(dataType);
            }

            _logger.LogInformation(ApiEventIds.ReadRelay, "Found DataType: Id {DataType}", dataType.Id);

            Report report = new()
            {
                DataType = dataType.Id,
                Device = device.Id,
                Posted = DateTime.UtcNow,
                Value = data.Value
            };

            _context.Reports.Add(report);

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ApiEventIds.UnknownException, ex, "Exception occured while saving changes from PostReport: {Data}", data);
                return BadRequest("An unknown error occured while processing the request");
            }

            _logger.LogInformation(ApiEventIds.CreateReport, "Successfully created Report {Id} from {Data}", report.Id, data);

            return CreatedAtAction(
                "GetReport", 
                new { id = report.Id },
                new DataReport
                {
                    DataType = dataType.Name,
                    DataUnits = dataType.Unit,
                    DeviceName = device.Name,
                    DeviceType = device.ConnectionType,
                    PostedOn = report.Posted,
                    RelayName = relay.Name,
                    Value = report.Value
                });
        }
    }
}