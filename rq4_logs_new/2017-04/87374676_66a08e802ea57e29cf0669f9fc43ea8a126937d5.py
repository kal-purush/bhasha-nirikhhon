
#to stakeholder-talk
#ask cits with [stakeholder? = 1] [
#  create-linkstakeholders-to cits with [stakeholder? = 1] with [who != [who] of myself]
#  [
#      set cbolink? ticks
#      set spref1 [sown-pref] of end1
#      set spower1 [sown-power] of end1
#      set seu1 [sown-eu] of end1
#      set spref2 [sown-pref] of end2
#      set spower2 [sown-power] of end2
#      set seu2 [sown-eu] of end2
#      set scbopref ((spref1 * spower1 + spref2 * spower2)/(spower1 + spower2 + 0.0000001))
#      set scbopower spower1 + spower2
#      ;;set scboeu scbopower * (100 - abs (scbopref - scbopref))
#      set scboeu1 (100 - abs(scbopref - spref1)) * (spower1 + (scbopower - spower1) * (spower1 / (scbopower + 0.000001)))
#      set scboeu2 (100 - abs(scbopref - spref2)) * (spower2 + (scbopower - spower2) * (spower2 / (scbopower + 0.000001)))
#      ask end1 [
#          if empty? [scboeu1] of my-out-links with [cbolink? = 3][
#               set stemp-eu 0
#          else:
#               set stemp-eu max [scboeu1] of my-out-links with [cbolink? = 3]
#      ]//end ask end1
#      ask end2 [
#          if empty? [scboeu1] of my-in-links with [cbolink? = 3][
#               set stemp-eu 0
#           else:
#               set stemp-eu max [scboeu2] of my-in-links with [cbolink? = 3]
#      ]//end ask end2
#      set hidden? TRUE
#]//End ask cits

#-------
#ask linkstakeholders with [cbolink? = ticks] [
#      if ([stemp-eu] of end1) + ([stemp-eu] of end2) != (scboeu1 + scboeu2) [
#          die
#      if ([stemp-eu] of end1 > [sown-eu] of end1) and ([stemp-eu] of end2 > [sown-eu] of end2)
#          ask end1 [
#          set sturcbo? 1
#          set sown-pref max [scbopref] of my-out-links with [cbolink? = ticks]
#          set own-pref sown-pref
#          set scbo-pref max [scbopref] of my-out-links with [cbolink? = ticks]
#          set scbo-power max [scbopower] of my-out-links with [cbolink? = ticks]
#          set sown-eu max [scboeu1] of my-out-links with [cbolink? = ticks]
#          set scbo-eu max [scboeu1] of my-out-links with [cbolink? = ticks]
#          ask end2 [
#          set sturcbo? 0
#          set sown-pref [scbo-pref] of other-end
#          set own-pref sown-pref
#          set scbo-pref [scbo-pref] of other-end
#          set scbo-power 0
#          set sown-eu max [scboeu2] of my-in-links with [cbolink? = ticks]
#          set scbo-eu max [scboeu2] of my-in-links with [cbolink? = ticks]
#          //LINKS
#          set hidden? FALSE
#          set color pink
#     else:
#       ask end1 [
#       set sown-pref sown-pref
#       set own-pref sown-pref
#       set sown-power sown-power
#       ask end2 [
#       set sown-pref sown-pref
#       set own-pref sown-pref
#       set sown-power sown-power

#----------
#ask linkstakeholders with [cbolink? < ticks] [
#       set spref1 [sown-pref] of end1
#       set spower1 [sown-power] of end1
#       set seu1 [sown-eu] of end1
#       set spref2 [sown-pref] of end2
#       set spower2 [sown-power] of end2
#       set seu2 [sown-eu] of end2
#       set scbopref ((spref1 * spower1 + spref2 * spower2)/(spower1 + spower2 + 0.0000001))
#       set scbopower spower1 + spower2
#       ;;set scboeu scbopower * (100 - abs (scbopref - scbopref))
#       set scboeu1 (100 - abs(scbopref - spref1)) * (spower1 + (scbopower - spower1) * (spower1 / (scbopower + 0.000001)))
#       set scboeu2 (100 - abs(scbopref - spref2)) * (spower2 + (scbopower - spower2) * (spower2 / (scbopower + 0.000001)))
#       if (scboeu1 < [sown-eu] of end1) or (scboeu2 < [sown-eu] of end2)
#          #ask end1 [
#          if count my-out-links with [cbolink? = 3] = 0 and count my-in-links with [cbolink? = ticks] = 0 [
#               set sturcbo? 0
#               set scbo-power 0
#          #ask end2 [
#          if count my-out-links with [cbolink? = 3] = 0 and count my-in-links with [cbolink? = ticks] = 0 [
#               set sturcbo? 0
#               set scbo-power 0]
#          die
#       else:
#          if [sown-pref] of end1 != [sown-pref] of end2 [
#               ask end1 [
#               if count my-out-links with [cbolink? = ticks] = 0 and count my-in-links with [cbolink? = ticks] = 0 [
#                   set sturcbo? 1
#                   set sown-pref [scbo-pref] of other-end
#                   set scbo-pref [scbo-pref] of other-end
#                   set scbo-power 0
#                   set sown-eu (100 - abs (sown-pref - sown-pref)) * sown-power]]
#               ask end2 [
#               if count my-out-links with [cbolink? = ticks] = 0 and count my-in-links with [cbolink? = ticks] = 0 [
#                   set sturcbo? 1
#                   set sown-pref [scbo-pref] of other-end
#                   set scbo-pref [scbo-pref] of other-end
#                   set scbo-power 0
#                   set sown-eu (100 - abs (sown-pref - sown-pref)) * sown-power]]]

#-----------------
#ask cits with [stakeholder? = 1] [
#  set scbo-power (sum [scbopower] of my-out-links with [cbolink? > 0]) + (sum [scbopower] of my-in-links with [cbolink? > 0]) - sown-power * ((count my-out-links with [cbolink? > 0]) + (count my-in-links with [cbolink? > 0]) - 1)]
#  ask cits with [stakeholder? = 1] [if (count my-out-links with [cbolink? >= 1] = 0) and (count my-in-links with [cbolink? >= 1] = 0)
#      set sturcbo? 0
#      set scbo-power 0
#end