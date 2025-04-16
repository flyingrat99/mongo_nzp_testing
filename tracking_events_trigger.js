exports = async function(changeEvent) {
  try {
    // Debug logging to see the entire change event structure
    console.log('Change event structure:', JSON.stringify(changeEvent, null, 2));
    
    // If changeEvent is not in the expected format, try to handle it differently
    if (!changeEvent.operationType) {
      console.log('No operation type found, trying to process as direct document');
      // Some triggers might receive the document directly
      const fullDocument = changeEvent;
      
      if (!fullDocument || !fullDocument.tracking_events) {
        console.log('Invalid document structure received:', JSON.stringify(fullDocument, null, 2));
        return;
      }
      
      // Continue processing with the document
      await processDocument(fullDocument);
      return;
    }
    
    console.log('Change event type:', changeEvent.operationType);
    
    // For updates, we need to check the updateDescription to see if tracking_events was modified
    if (changeEvent.operationType === 'update') {
      const updatedFields = changeEvent.updateDescription.updatedFields;
      // Check if any of the updated fields involve tracking_events
      const hasTrackingEventsUpdate = Object.keys(updatedFields).some(field => 
        field === 'tracking_events' || field.startsWith('tracking_events.'));
      
      if (!hasTrackingEventsUpdate) {
        console.log('No tracking_events updates in this change');
        return;
      }
    }

    // Get the full document
    const fullDocument = changeEvent.fullDocument;
    if (!fullDocument) {
      console.log('No full document available in change event');
      return;
    }

    await processDocument(fullDocument);
    
  } catch (error) {
    console.error('Trigger error:', error);
    throw error;
  }
};

async function processDocument(fullDocument) {
  // Check if tracking_events exists and is an array
  if (!fullDocument.tracking_events || !Array.isArray(fullDocument.tracking_events) || fullDocument.tracking_events.length === 0) {
    console.log(`No tracking events found for document with tracking_reference: ${fullDocument.tracking_reference}`);
    return;
  }
  
  // Get the database and collections using the correct service name
  const serviceName = "ParcelTrackUAT";
  const database = "parceltrackuat";
  
  const collection = context.services
    .get(serviceName)
    .db(database);
  
  const parcelItemEventCollection = collection.collection("parcel_item_event");
  
  // Get the most recent tracking event
  const mostRecentEvent = fullDocument.tracking_events.reduce((latest, current) => {
    const currentDate = new Date(current.event_datetime);
    const latestDate = new Date(latest.event_datetime);
    return currentDate > latestDate ? current : latest;
  });
  
  // Verify mostRecentEvent has the required fields
  if (!mostRecentEvent || !mostRecentEvent.event_datetime) {
    console.log(`Invalid event structure found for tracking_reference: ${fullDocument.tracking_reference}`);
    return;
  }

  // Get the tpid directly from the event object
  const tpid = mostRecentEvent.tpid;
  
  // Skip if tpid is not present
  if (!tpid) {
    console.log(`Skipping trigger for tracking_reference ${fullDocument.tracking_reference} - no tpid found`);
    return;
  }
  
  // Create the latest_event object structure
  const latestEvent = {
    event_datetime: mostRecentEvent.event_datetime,
    depot_name: mostRecentEvent.depot_name || "",
    event_code: mostRecentEvent.event_code,
    exported_event_code: mostRecentEvent.exported_event_code,
    event_edifact_code: mostRecentEvent.event_edifact_code,
    location: mostRecentEvent.location || {},
    run_name: mostRecentEvent.run_name || "",
    event_type: mostRecentEvent.event_type || "",
    event_description: mostRecentEvent.event_description || "",
    reason_status: mostRecentEvent.reason_status || "",
    seqref: mostRecentEvent.seqref || "",
    signed_by: mostRecentEvent.signed_by || {},
    source: mostRecentEvent.source,
    metadata: mostRecentEvent.metadata
  };
  
  // Check if document exists in parcel_item_event collection
  const existingDoc = await parcelItemEventCollection.findOne({
    tracking_reference: fullDocument.tracking_reference
  });
  
  if (existingDoc) {
    // Compare event dates
    const existingEventDate = new Date(existingDoc.latest_event.event_datetime);
    const newEventDate = new Date(mostRecentEvent.event_datetime);
    
    if (newEventDate > existingEventDate) {
      // Update the existing document
      await parcelItemEventCollection.updateOne(
        { tracking_reference: fullDocument.tracking_reference },
        {
          $set: {
            tpid: tpid,
            latest_event: latestEvent
          }
        }
      );
      console.log(`Updated document for tracking_reference: ${fullDocument.tracking_reference}`);
    } else {
      console.log(`No update needed - existing event is more recent for tracking_reference: ${fullDocument.tracking_reference}`);
    }
  } else {
    // Create new document
    await parcelItemEventCollection.insertOne({
      tracking_reference: fullDocument.tracking_reference,
      tpid: tpid,
      latest_event: latestEvent,
      item: {}
    });
    console.log(`Created new document for tracking_reference: ${fullDocument.tracking_reference}`);
  }
} 