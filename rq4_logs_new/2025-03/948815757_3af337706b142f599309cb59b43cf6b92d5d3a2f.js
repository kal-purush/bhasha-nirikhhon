// Local storage helper for Event Alert Extension
const storageHelper = {
    // Add event to monitor
    addEvent: async (url, eventName, eventDate, dateSelector, notificationTriggers) => {
      try {
        // Generate a unique ID for the event
        const eventId = 'event_' + Date.now();
        
        // Calculate days until event
        const today = new Date();
        const eventDateObj = new Date(eventDate);
        const daysUntil = Math.ceil((eventDateObj - today) / (1000 * 60 * 60 * 24));
        
        // Create event object
        const eventData = {
          id: eventId,
          url: url,
          eventName: eventName,
          eventDate: eventDate,
          dateSelector: dateSelector,
          daysUntil: daysUntil,
          createdAt: new Date().toISOString(),
          lastChecked: null,
          enabled: true,
          notificationTriggers: notificationTriggers || {
            oneWeekBefore: true,
            threeDaysBefore: true,
            oneDayBefore: true
          }
        };
        
        // Get existing events
        const events = await storageHelper.getEvents();
        events.push(eventData);
        
        // Save updated events list
        await chrome.storage.sync.set({ 'monitoredEvents': events });
        
        return eventId;
      } catch (error) {
        console.error("Error adding event:", error);
        throw error;
      }
    },
    
    // Get all events
    getEvents: async () => {
      try {
        return new Promise(resolve => {
          chrome.storage.sync.get('monitoredEvents', (result) => {
            resolve(result.monitoredEvents || []);
          });
        });
      } catch (error) {
        console.error("Error getting events:", error);
        return [];
      }
    },
    
    // Update event
    updateEvent: async (eventId, updatedData) => {
      try {
        // Get all events
        const events = await storageHelper.getEvents();
        
        // Find and update the specified event
        const updatedEvents = events.map(event => {
          if (event.id === eventId) {
            return { ...event, ...updatedData };
          }
          return event;
        });
        
        // Save updated events list
        await chrome.storage.sync.set({ 'monitoredEvents': updatedEvents });
      } catch (error) {
        console.error("Error updating event:", error);
        throw error;
      }
    },
    
    // Delete event
    deleteEvent: async (eventId) => {
      try {
        // Get all events
        const events = await storageHelper.getEvents();
        
        // Filter out the event to delete
        const updatedEvents = events.filter(event => event.id !== eventId);
        
        // Save updated events list
        await chrome.storage.sync.set({ 'monitoredEvents': updatedEvents });
      } catch (error) {
        console.error("Error deleting event:", error);
        throw error;
      }
    },
    
    // Get user preferences
    getPreferences: async () => {
      try {
        return new Promise(resolve => {
          chrome.storage.sync.get('userPreferences', (result) => {
            if (result.userPreferences) {
              resolve(result.userPreferences);
            } else {
              // Default preferences
              const defaultPreferences = {
                emailNotifications: true,
                notificationEmail: '',
                notificationFrequency: 'automatic',
                browserNotifications: true
              };
              
              // Save default preferences
              chrome.storage.sync.set({ 'userPreferences': defaultPreferences });
              resolve(defaultPreferences);
            }
          });
        });
      } catch (error) {
        console.error("Error getting preferences:", error);
        return {
          emailNotifications: true,
          notificationEmail: '',
          notificationFrequency: 'automatic',
          browserNotifications: true
        };
      }
    },
    
    // Update user preferences
    updatePreferences: async (updatedPreferences) => {
      try {
        // Get current preferences
        const currentPreferences = await storageHelper.getPreferences();
        
        // Merge with updated preferences
        const newPreferences = { ...currentPreferences, ...updatedPreferences };
        
        // Save updated preferences
        await chrome.storage.sync.set({ 'userPreferences': newPreferences });
      } catch (error) {
        console.error("Error updating preferences:", error);
        throw error;
      }
    }
  };