# mqtt_example
Example mqtt worker library using paho

On startup the worker will do discovery to see if there is another worker of the same name ( ie. function )
and if there is it will go into backup mode. if the other worker dies this will assume the primary role

Misc Notes:

    send_admin_email was a custom webhook to a local node



ACL for MQTT Library:

    workers/# pub sub

