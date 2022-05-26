use admin
db.auth("root","root")

use patient
db.createUser({user:"doctor", pwd:"doctor", roles: [{role: "read", db:"patient"}]})

db.createRole( {role:"writecollDoctor", privileges: [ {resource:{db: "patient", collection:"medical_actions"},
  actions: ["find", "update", "insert", "remove",  "renameCollectionSameDB","dropCollection","createIndex"]}],roles:[] })

db.grantRolesToUser("doctor",["writecollDoctor"])



db.createUser({user:"care_maker", pwd:"care_maker", roles:[]})


db.createRole({role:"caremakerRole", privileges: [ {resource:{db: "patient", collection:"sensors_logs"},
  actions: ["find","collStats"]}, {resource:{db: "patient", collection:"medical_actions"},
  actions: ["find", "update", "insert", "remove"]} ],roles:[]})

db.grantRolesToUser("care_maker",["caremakerRole"])
