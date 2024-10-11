# Kafka Connect for SPaT Data Storage

This script allows you to connect to a Kafka stream and store SPaT (Signal Phasing and Timing) data for a specific intersection in a specified output folder.

## Usage

To run the script, use the following command:

```bash
python kafka_connect.py --output_folder <output_folder> --key <key> 
```
Example command:
```bash
python kafka_connect.py --output_folder 2024-10-11 --key ca-long-beach-052
```

## more details
Intersection Keys
Here is the list of available intersection keys:
* 36th Street: ca-long-beach-058
* 37th Street: ca-long-beach-055
* Bixby: ca-long-beach-052
* Roosevelt: ca-long-beach-049
* Marshall: ca-long-beach-056

Quick Link to Corridor on [Google Maps](https://www.google.com/maps/place/Atlantic+%26+Bixby+SE/@33.8306672,-118.1949731,15z/data=!4m10!1m2!2m1!1sBixby+%26+Atlantic!3m6!1s0x80dd339849c22dfb:0x2e1fb0b345b7e155!8m2!3d33.827398!4d-118.184881!15sChBCaXhieSAmIEF0bGFudGljkgEIYnVzX3N0b3DgAQA!16s%2Fg%2F1tcxvk3z?entry=ttu&g_ep=EgoyMDI0MTAwOC4wIKXMDSoASAFQAw%3D%3D)


## Notes
Make sure to replace <output_folder> with your desired folder and <key> with the key of the specific intersection.
The script stores SPaT data for the specified intersection key in the provided folder.