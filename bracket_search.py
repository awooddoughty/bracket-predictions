import requests as req
import random
import json

base = "http://g.espncdn.com/tournament-challenge-bracket/2018/en/api/group?groupID={}"

# 1041234

minimum = 0
maximum = 3200000
span_unit = 100000
samples_per_span = 100

valid = []
for i in range(minimum, maximum + 1, span_unit):
    good, bad, err = 0., 0., 0.
    gids = random.sample(range(i, i + span_unit), samples_per_span)
    for gid in gids:
        resp = req.get(base.format(gid))
        try:
            obj = json.loads(resp.text)
            if len(obj) > 0:
                valid.append(str(gid))
                good += 1
            else:
                bad += 1
        except:
          err += 1
    print("{}: {:.1f}% good".format(i, 100 * good / samples_per_span))
    if err > 0:
        print("{} errors!".format(err))


with open("valid_ids.txt", "a") as outf:
    outf.write("\n".join(valid) + "\n")
