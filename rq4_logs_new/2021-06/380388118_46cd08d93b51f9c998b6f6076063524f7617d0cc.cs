using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using IoT_Environment.Models;
using IoT_Environment.DTO;
using IoT_Environment.Extensions;
using Microsoft.Extensions.Logging;
using IoT_Environment.Logging;

namespace IoT_Environment.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DevicesController : ControllerBase
    {
        private readonly IoTContext _context;
        private readonly ILogger<DevicesController> _logger;

        public DevicesController(IoTContext context, ILogger<DevicesController> logger)
        {
            _context = context;
            _logger = logger;
        }

        // GET: api/Devices
        [HttpGet]
        public async Task<ActionResult<IEnumerable<Device>>> GetDevices()
        {
            _logger.LogInformation(ApiEventIds.ReadAllDevices, "Getting all Devices");
            List<Device> devices = await _context.Devices.ToListAsync();

            _logger.LogInformation(ApiEventIds.ReadAllDevices, "Found {Count} Devices", devices.Count);
            return devices;
        }

        // GET: api/Devices/5
        [HttpGet("{id}")]
        public async Task<ActionResult<Device>> GetDevice(int id)
        {
            _logger.LogInformation(ApiEventIds.ReadDevice, "Searching for Device with Id {Id}", id);
            var device = await _context.Devices.FindAsync(id);

            if (device == null)
            {
                _logger.LogInformation(ApiEventIds.ReadDevice, "Could not find Device {Id}", id);
                return NotFound($"Could not find Device with Id {id}");
            }

            _logger.LogInformation(ApiEventIds.ReadDevice, "Found Device {Id}: {Address}", device.Id, device.Address);
            return device;
        }

        // PUT: api/Devices/5
        [HttpPut("{id}")]
        public async Task<IActionResult> PutDevice(int id, DeviceRequest request)
        {
            _logger.LogInformation(ApiEventIds.UpdateDevice, "Starting update for Device Id {Id}", id);
            if (id != request.Id)
            {
                _logger.LogInformation(ApiEventIds.UpdateDevice, "Device update failed -- Id mismatch: {ResourceId}, {RequestId}", id, request.Id);
                return BadRequest($"Request Id mismatch: {id}, {request.Id}");
            }

            Device device = await _context.Devices.FindAsync(id);
            if (device == null)
            {
                _logger.LogInformation(ApiEventIds.UpdateDevice, "Could not find Device {Id}", id);
                return NotFound($"Could not find Device with Id {id}");
            }

            _context.Entry(device).Reference(d => d.RelayNavigation).Load();
            Relay relay = device.RelayNavigation;

            if (relay.TryUpdateNetworkAddress(request.RelayNetworkAddress))
            {
                _logger.LogInformation(ApiEventIds.UpdateDevice, "Updating Relay network address: {Old} -> {New}", relay.NetworkAddress, request.RelayNetworkAddress);
                _context.Entry(relay).State = EntityState.Modified;
            }

            device.Name = request.Name;
            device.RelayNavigation = relay;
            device.Description = request.Description;
            device.Address = request.Address;
            device.ConnectionType = request.ConnectionType;

            _context.Entry(device).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ApiEventIds.UnknownException, ex, "Exception occured while saving changes from PutDevice for Device {Id}", device.Id);
                return BadRequest("An unknown error occured while processing the request");
            }

            _logger.LogInformation(ApiEventIds.UpdateDevice, "Update for Device {Id} complete", id);
            return NoContent();
        }

        // POST: api/Devices
        [HttpPost]
        public async Task<ActionResult<Device>> PostDevice(DeviceRequest request)
        {
            _logger.LogInformation(ApiEventIds.CreateDevice, "Starting Device registration for {Address} on Relay {PhysAddr}", request.Address, request.RelayPhysicalAddress);

            Relay relay = await _context.Relays.FirstOrDefaultAsync(r => r.PhysicalAddress == request.RelayPhysicalAddress);
            if (relay == null)
            {
                _logger.LogInformation(ApiEventIds.CreateDevice, "Could not find Relay {Addr}", request.RelayPhysicalAddress);
                return NotFound($"Could not find Relay with network address {request.RelayPhysicalAddress}");
            }

            Device device = await _context.Devices.FirstOrDefaultAsync(d => d.Address == request.Address && d.Relay == relay.Id);
            if (device != null)
            {
                _logger.LogInformation(ApiEventIds.CreateDevice, "Failed registering Device: {Address} already exists on Relay {PhysAddr}", request.Address, request.RelayPhysicalAddress);
                return Conflict($"Device with address {request.Address} already exists on Relay {relay.PhysicalAddress}");
            }

            if (relay.TryUpdateNetworkAddress(request.RelayNetworkAddress))
            {
                _logger.LogInformation(ApiEventIds.CreateDevice, "Updating Relay network address: {Old} -> {New}", relay.NetworkAddress, request.RelayNetworkAddress);
                _context.Entry(relay).State = EntityState.Modified;
            }

            device = new Device
            {
                Address = request.Address,
                Name = request.Name,
                Description = request.Description,
                ConnectionType = request.ConnectionType,
                DateRegistered = DateTime.UtcNow,
                Relay = relay.Id,
                Active = true,
            };

            _context.Devices.Add(device);

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ApiEventIds.UnknownException, ex, "Exception occured while saving changes from PostDevice for Device {Id}", device.Id);
                return BadRequest("An unknown error occured while processing the request");
            }

            _logger.LogInformation(ApiEventIds.CreateDevice, "Successfully created Device {Address} with Id {Id}", device.Address, device.Id);
            return CreatedAtAction("GetDevice", new { id = request.Id }, request);
        }

        // DELETE: api/Devices/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteDevice(int id)
        {
            _logger.LogInformation(ApiEventIds.DeleteDevice, "Starting delete for Device Id {Id}", id);

            var device = await _context.Devices.FindAsync(id);
            if (device == null)
            {
                _logger.LogInformation(ApiEventIds.DeleteDevice, "Could not find Device {Id}", id);
                return NotFound($"Could not find Device with Id {id}");
            }

            _context.Devices.Remove(device);

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ApiEventIds.UnknownException, ex, "Exception occured while saving changes from DeleteDevice for Device {Id}", device.Id);
                return BadRequest("An unknown error occured while processing the request");
            }

            _logger.LogInformation(ApiEventIds.DeleteDevice, "Successfully deleted Device {Address} with Id {Id}", device.Address, device.Id);
            return NoContent();
        }
    }
}