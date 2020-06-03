import 'dotenv/config'
import { RingApi } from 'ring-client-api'
import { skip } from 'rxjs/operators'
import { readFile, writeFile } from 'fs'
import { promisify } from 'util'
const fs = require('fs'),
  path = require('path'),
  express = require('express')

import { BaseStation } from './homebridge/base-station'

async function example() {
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


  ringApi.onRefreshTokenUpdated.subscribe(
    async ({ newRefreshToken, oldRefreshToken}) => {
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
        //console.log("here camera data", data);
      })
      camera.startVideoOnDemand();
      console.log(`here camera pro- ${camera.id}: ${camera.name} (${camera.deviceType})`)
    }

    console.log(
      `\nLocation ${location.name} has the following ${devices.length} device(s):`
    )

    for (const device of devices) {
      console.log(`- ${device.zid}: ${device.name} (${device.deviceType})`) 
    }
  }

  if (allCameras.length) {
    allCameras.forEach((camera) => {
      camera.onNewDing.subscribe((ding) => {
        const event =
          ding.kind === 'motion'
            ? 'Motion detected'
            : ding.kind === 'ding'
            ? 'Doorbell pressed'
            : `Video started (${ding.kind})`
        console.log("here event", event);
        console.log(
          `${event} on ${camera.name} camera. Ding id ${
            ding.id_str
          }.  Received at ${new Date()
          }.  ${ding.sip_server_ip
          }.  ${ding.sip_server_port
          }.  ${ding.sip_token
          }.  ${ding.sip_session_id
          }.  ${ding.sip_ding_id
          }.  ${ding.sip_to
          }.  ${ding.sip_from
          }.  "here snapshort" ${ding.snapshot_url
          }.  ${ding.device_kind
          }.  "here kind" ${ding.kind
          }.  ${ding.sip_endpoints
          }.  "here id str" ${ding.id_str
          }.  "here state" ${ding.state
          }.  "here protocol ${ding.protocol
          }.  "here doorbot_description" ${ding.doorbot_description
          }.  "here device_kind" ${ding.device_kind
          }.  "here motion" ${ding.motion
          }.  "here sip token" ${ding.sip_token
          }.  "here sip ding id" ${ding.sip_ding_id}`
        )
      })
    })

    console.log('Listening for motion and doorbell presses on your cameras.')
  }

}

example()
