import { Instance, types, getRoot } from "mobx-state-tree"
import { EnrichKoodiUri } from "models/EnrichKoodiUri"
import { EPerusteetVastaus } from "models/EPerusteetVastaus"
import { LocaleRoot } from "models/helpers/LocaleRoot"

export type ShareType = "naytto" | "tyossaoppiminen"

const NotificationModel = types.model("NotificationModel", {
  id: types.optional(types.number, 0),
  hoksId: types.optional(types.string, ""),
  tutkinnonOsaKoodiUri: types.optional(types.string, ""),
  tutkinnonOsa: types.optional(EPerusteetVastaus, {}),
  tyyppi: types.enumeration("tyyppi", ["naytto", "tyossaoppiminen"]),
  alku: types.optional(types.string, ""),
  loppu: types.optional(types.string, ""),
  paikka: types.optional(types.string, ""),
  visible: types.optional(types.boolean, true)
})

const Notification = types
  .compose(
    "Notification",
    EnrichKoodiUri,
    NotificationModel
  )
  .views(self => {
    const root: LocaleRoot = getRoot(self)
    return {
      get message() {
        return {
          id: self.id,
          hoksId: self.hoksId,
          koodiUri: self.tutkinnonOsaKoodiUri,
          title: self.tutkinnonOsa.nimi[root.translations.activeLocale],
          type: self.tyyppi,
          location: self.paikka,
          startDate: self.alku,
          endDate: self.loppu
        }
      }
    }
  })
  .actions(self => {
    const ackNotification = function() {
      // TODO: call API to mark notification as acknowledged
      console.log("Notification acknowledged")
    }

    const hide = () => {
      self.visible = false
    }
    return { ackNotification, hide }
  })

export const NotificationStore = types
  .model("NotificationStore", {
    isLoading: types.optional(types.boolean, false),
    notifications: types.array(Notification)
  })
  .actions(self => {
    // const { apiUrl, fetchCollection, errors } = getEnv<StoreEnvironment>(self)

    //const haeMuistutukset = flow(function*(apiUrl: (path: string) => string) {
    const fetchNotifications = function() {
      self.isLoading = true

      // TODO: mock data, replace with real API call
      self.notifications.replace([
        {
          id: 1,
          hoksId: "712fd599-5985-45d6-9787-aa782ea4e553",
          tutkinnonOsaKoodiUri: "tutkinnonosat_103590",
          tyyppi: "naytto",
          alku: "2019-08-01",
          loppu: "2019-08-04",
          paikka: "Koivikkola"
        } as Instance<typeof Notification>
      ])
      // try {
      //   const response = yield fetchCollection(apiUrl("muistutukset"))
      //   self.notifications.replace(response.data)
      // } catch (error) {
      //   errors.logError("NotificationStore.haeMuistutukset", error.message)
      // }
      self.isLoading = false
    }

    return { fetchNotifications }
  })
  .views(self => {
    return {
      get visible(): Array<Instance<typeof Notification>> {
        return self.notifications.filter(notification => notification.visible)
      }
    }
  })

export interface INotificationStore
  extends Instance<typeof NotificationStore> {}