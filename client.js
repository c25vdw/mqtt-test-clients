var mqtt = require("mqtt")
const cluster = require("cluster")
const cmdArgs = require("command-line-args")
const { cpus } = require("os")
const cmdOptions = [
    {
        name: "host",
        alias: "h",
        type: String,
        defaultValue: "localhost",
    },
    {
        name: "port",
        alias: "p",
        type: Number,
        defaultValue: 1883,
    },
    {
        name: "num",
        alias: "n",
        type: Number,
        defaultValue: 1,
    },
    {
        name: "qos",
        alias: "q",
        type: Number,
        defaultValue: 1,
    },
    {
        name: "payload_size",
        alias: "P",
        type: Number,
        defaultValue: 10,
    },
]

const options = cmdArgs(cmdOptions)

function initClients(options) {
    let clients = []
    for (let i = 0; i < options.num; i++) {
        let client = mqtt.connect(`mqtt://${options.host}:${options.port}`)

        client.on("connect", function () {
            client.subscribe(`resp/${client.options.clientId}`, function (err) {
                if (err) {
                    console.log("subscribe error: ", err)
                }
            })
        })

        clients.push(client)
    }
    return clients
}

async function startPolling(clients, options) {
    let totalLatency = 0.0
    let counter = 0
    promises = clients.map((c) => {
        return new Promise((res, rej) => {
            var now
            c.on("message", function () {
                let diff = Date.now() - now
                res(diff)
            })
            let payload = {
                cid: c.options.clientId,
                extra: "a".repeat(options.payload_size),
            }
            c.publish(`req/${c.options.clientId}`, JSON.stringify(payload))
            now = Date.now() // ms
        })
    })
    let diffs = await Promise.all(promises)
    // console.log(diffs)
    totalLatency += diffs.reduce((prev, curr, i) => prev + curr)
    counter += diffs.length
    clients.forEach((c) => c.end())
    //     console.log(
    //         "total: ",
    //         totalLatency,
    //         " count: ",
    //         counter,
    //         " avg: ",
    //         totalLatency / counter
    //     )
    return [totalLatency, counter]
}

if (cluster.isPrimary) {
    let total = 0
    let counter = 0
    for (let i = 0; i < cpus().length; i++) {
        cluster.fork()
    }

    for (const id in cluster.workers) {
        cluster.workers[id].on("message", (msg) => {
            total += msg.total
            counter += msg.counter
            console.log("total: ", total, "avg: ", total / counter)
        })
    }
} else {
    options.num = parseInt(1 + options.num / cpus().length)
    let clients = initClients(options)
    startPolling(clients, options).then(([totalLatency, counter]) => {
        // console.log(totalLatency)

        process.send({ total: totalLatency, counter })
    })
}
