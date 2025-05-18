Custom script to parse dump1090 messages from an ADSB receiver, extract aircraft positions, and process them for downstream dashboarding.

The ADSB receiver is connected to a Raspberry Pi running dump1090, which is a software that decodes ADS-B messages from aircraft.
The process to set up the raspberry pi and dump1090 is outlined here:
https://www.raspberrypi.com/tutorials/build-your-own-raspberry-pi-flight-tracker/

The script does the following:
- Parse the dump1090 messages from the ADSB receiver
- Cycle through the messages to find each aircraft with position information
- Store aircraft information in a dictionary if it is not already present
- Update the aircraft information if the latest data shows it is the furthest position
  - We only want to display the furthest position of each unique aircraft spotted
- A post-processor parses the aircraft dictionary and creates a CSV file with the aircraft information
- A dashboarding tool (Tableau) is used to visualize the data

Tableau Link (v1.0):
(to be added when v1.0 is available)
