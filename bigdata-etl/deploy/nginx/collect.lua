local cjson = require "cjson.safe"

ngx.req.read_body()
local body = ngx.req.get_body_data()
if not body then
    ngx.status = 400
    ngx.say('{"code":400,"msg":"empty body"}')
    return
end

local event = cjson.decode(body)
if not event or not event.appId or not event.eventName then
    ngx.status = 400
    ngx.say('{"code":400,"msg":"invalid event"}')
    return
end

event.receiveTime = ngx.now() * 1000
event.eventId = event.eventId or ngx.md5((event.appId or "") .. "|" .. (event.userId or "") .. "|" .. (event.sessionId or "") .. "|" .. (event.eventName or "") .. "|" .. tostring(event.eventTime or event.receiveTime))
event.ip = ngx.var.remote_addr
event.userAgent = ngx.req.get_headers()["user-agent"]

local payload = cjson.encode({
    records = {
        {
            key = event.appId,
            value = event
        }
    }
})

local res = ngx.location.capture("/_kafka/topics/behavior_raw", {
    method = ngx.HTTP_POST,
    body = payload
})

if not res then
    ngx.status = 503
    ngx.say(cjson.encode({ code = 503, msg = "kafka rest proxy unavailable" }))
    return
end

ngx.status = res.status >= 300 and 502 or 200
ngx.say(cjson.encode({ code = ngx.status, eventId = event.eventId }))
