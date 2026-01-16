using HOTELAPI1.Models;
using System.Text.Json;

namespace HOTELAPI1.Services
{
    public class ReservacionService
    {
        private readonly HotelDbContext _context;

        public ReservacionService(HotelDbContext context)
        {
            _context = context;
        }
        public async Task InsertDataFromJsonAsync(string jsonData)
        {
            try
            {
                var reservaciones = JsonSerializer.Deserialize<List<Reservacion>>(jsonData);
                if (reservaciones == null || reservaciones.Count == 0)
                {
                    throw new Exception("No data to import");
                }

                await _context.Reservaciones.AddRangeAsync(reservaciones);
                await _context.SaveChangesAsync();
            }
            catch (JsonException ex)
            {
                throw new Exception($"Invalid JSON format: {ex.Message}", ex);
            }
            catch (Exception ex)
            {
                throw new Exception($"Database operation failed: {ex.Message}", ex);
            }
        }
    }
}


