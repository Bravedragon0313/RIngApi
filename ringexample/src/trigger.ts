import 'dotenv/config'
import { RingApi } from 'ring-client-api'
import { skip } from 'rxjs/operators'
import { readFile, writeFile } from 'fs'
import { promisify } from 'util'
const fs = require('fs')
import "reflect-metadata";
import { createConnection } from "typeorm";
import { Devices } from "./entity/Devices";

async function trigger() {
  //var sipdata = new Array();
  var count;
  const { env } = process,
    ringApi = new RingApi({
      // Replace with your refresh token
      refreshToken: env.RING_REFRESH_TOKEN!,
      // Listen for dings and motion events
      cameraDingsPollingSeconds: 2,
    }),
    locations = await ringApi.getLocations(),
    allCameras = await ringApi.getCameras()

  console.log(
    `Found ${locations.length} location(s) with ${allCameras.length} camera(s).`
  )
  ringApi.onRefreshTokenUpdated.subscribe(
    async ({ newRefreshToken, oldRefreshToken }) => {
      console.log('Refresh Token Updated: ', newRefreshToken)
 
      if (!oldRefreshToken) {
        return
      }
  
      const currentConfig = await promisify(readFile)('.env'),
        updatedConfig = currentConfig
          .toString()
          .replace(oldRefreshToken, newRefreshToken)

      await promisify(writeFile)('.env', updatedConfig)
    }
  )

  for (const location of locations) {
    location.onConnected.pipe(skip(1)).subscribe((connected) => {
      const status = connected ? 'Connected to' : 'Disconnected from'
      console.log(`**** ${status} location ${location.name} - ${location.id}`)
    })
  }

  for (const location of locations) {
    const cameras = location.cameras,
    devices = await location.getDevices();
    console.log("here device location", devices);
    console.log(
      `\nLocation ${location.name} has the following ${cameras.length} camera(s):`
    )

    for (const camera of cameras) {
      camera.onData.subscribe(data => {
        // console.log("here camera data", data);
        // console.log("device id", data.device_id);
        //sipdata.push('[ring'+ringcount+']');
      })
      camera.startVideoOnDemand();
 
      console.log(`here camera pro- ${camera.id}: ${camera.name} (${camera.deviceType})`)
    }
 
    console.log(
      `\nLocation ${location.name} has the following ${devices.length} device(s):`
    )

    for (const device of devices) {
      console.log(`- ${device.zid}: ${device.name} (${device.deviceType})`)
      //sipdata.push('here deviceid:', device.id, device.name,device.deviceType)
    }
  }

  count = 0;
  console.log("here conut", count);

  if (allCameras.length) {
    
    allCameras.forEach((camera) => {
      count++;
      var ringcount = String(count).padStart(2, '0')
      camera.onNewDing.subscribe((ding) => {
        console.log("here ding kind", ding.kind)
        const event =
          ding.kind === 'motion'
            ? 'Motion detected'
            : ding.kind === 'ding'
              ? 'Doorbell pressed'
              : `Video started (${ding.kind})`
       // console.log("here event", event);
        console.log(
          `${event} on ${camera.name} camera. Ding id ${
          ding.id_str
          }. Received at ${new Date()
          }. "here server ip:"${ding.sip_server_ip
          }. "here server port:"${ding.sip_server_port
          }. "here sip token:"${ding.sip_token
          }. "here sip session id:"${ding.sip_session_id
          }. "here sip ding id:"${ding.sip_ding_id
          }. "here sip to:"${ding.sip_to.split(":")[2].split(';')[0]
          }. "here sip from:"${ding.sip_from
          }. "here snapshort:" ${ding.snapshot_url
          }. "here device kind:" ${ding.device_kind
          }. "here kind:" ${ding.kind
          }. "here endpoints:" ${ding.sip_endpoints 
          }. "here id str:" ${ding.id_str
          }. "here state:" ${ding.state
          }. "here protocol:" ${ding.protocol
          }. "here doorbot_description:" ${ding.doorbot_description
          }. "here device_kind:" ${ding.device_kind
          }. "here motion:" ${ding.motion
          }. "here sip token:" ${ding.sip_token
          }. "here sip ding id:" ${ding.sip_ding_id}`
        )
        console.log("here ringcount", ringcount)
        //var path = '../../../../../../../etc/asterisk/ring.d/'+'ring' + ringcount + '.conf';
        var path = 'ring' + ringcount + '.conf';
 
        try {
          if (fs.existsSync(path))  {
            fs.unlinkSync(path)
            //file removed
            promisify(writeFile)(path, '[ring' + ringcount + ']' + '\n' + 'type=aor' + '\n'
            + 'contact=sip:' + ding.sip_server_ip + ':' + ding.sip_to.split(":")[2].split(';')[0] + '\n' + '[ring' + ringcount + ']' + '\n' + 'type = endpoint' + '\n'
            + 'transport=transport-tls' + '\n' + 'ice_support = yes' + '\n' + 'allow=!all,ulaw,alaw,h264' + '\n' + 'aors=' + 'ring' + ringcount + '\n'
            + 'media_encryption=dtls' + '\n' + 'media_encryption=sdes' + '\n' + 'from_user=' + ding.sip_from.substring(4, 13) + '\n' + 'from_domain=ring.com' + '\n'
            + 'outbound_proxy=sip:' + ding.sip_server_ip + ':' + ding.sip_to.split(":")[2].split(';')[0] + '\n' + 'direct_media=no' + '\n' + 'direct_media_method=invite' + '\n'
            + 'dtmf_mode=info' + '\n' + 'use_avpf=yes')
          }else{
            promisify(writeFile)(path, '[ring' + ringcount + ']' + '\n' + 'type=aor' + '\n'
            + 'contact=sip:' + ding.sip_server_ip + ':' + ding.sip_to.split(":")[2].split(';')[0] + '\n' + '[ring' + ringcount + ']' + '\n' + 'type = endpoint' + '\n'
            + 'transport=transport-tls' + '\n' + 'ice_support = yes' + '\n' + 'allow=!all,ulaw,alaw,h264' + '\n' + 'aors=' + 'ring' + ringcount + '\n'
            + 'media_encryption=dtls' + '\n' + 'media_encryption=sdes' + '\n' + 'from_user=' + ding.sip_from.substring(4, 13) + '\n' + 'from_domain=ring.com' + '\n'
            + 'outbound_proxy=sip:' + ding.sip_server_ip + ':' + ding.sip_to.split(":")[2].split(';')[0] + '\n' + 'direct_media=no' + '\n' + 'direct_media_method=invite' + '\n'
            + 'dtmf_mode=info' + '\n' + 'use_avpf=yes')
          }
          this.allCameras
        } catch (err) {
          console.error(err)
        }
        createConnection({
          type: "mysql",
          host: "localhost",
          port: 3306,
          // username: "ivan",
          // password: "ELb8pCtVhHlcIjki",
          username: "root",
          password: "",
          database: "ringd",
          entities: [
              __dirname + "/entity/*.ts"
          ],
          synchronize: true,
          logging: false
        }).then(async connection => {
     
          console.log("Inserting a new devices into the database...");
          const device = new Devices();
          console.log("here event for test", event)
          device.deviceid = 'ring' + ringcount;
          device.devicename = camera.name;
          device.motion_extension = ding.motion;
          if (ding.kind === 'motion') {
            device.buttonpush_extension = false;
            device.liveview_extension = false;
          } else if (event === 'Doorbell pressed') {
            device.buttonpush_extension = true;
            device.liveview_extension = false;
          } else {
            device.buttonpush_extension = false;
            device.liveview_extension = true;
          }
          // console.log("here database device", device)

          // await connection.manager.remove(device).then(result => {
          //   console.log(result);})
            
          await connection.manager.save(device).then(result => {
            //console.log(result);
    
            //console.log("Loading devices from the database...");
            connection.manager.find('devices').then(devices => {
            //console.log("Loaded devices: ", devices);
            });
          });

        }).catch(error => console.log(error));
        
      })
    })
    console.log('Listening for motion and doorbell presses on your cameras.')
  }

}

trigger()
