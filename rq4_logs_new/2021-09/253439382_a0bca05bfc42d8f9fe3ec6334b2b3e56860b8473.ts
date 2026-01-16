import { flushPromises, shallowMount } from "@vue/test-utils";
import ComplexMembers from "@/components/concept//members/ComplexMembers.vue";
import Dialog from "primevue/dialog";
import Button from "primevue/button";
import ProgressSpinner from "primevue/progressspinner";
import EntityService from "@/services/EntityService";
import LoggerService from "@/services/LoggerService";

describe("ComplexMembers.vue", () => {
  let wrapper: any;
  let mockToast: any;

  beforeEach(async () => {
    jest.resetAllMocks();

    EntityService.getComplexMembers = jest.fn().mockResolvedValue({
      data: [
        '<p class="intersection">Intersection of</p><p class="name" style="margin-left: 40px;">UK product (product)</p><p class="having" style="margin-left: 40px;">having</p> <p class="role-group" style="margin-left: 80px">Has VMP (attribute)->COVID-19 vaccine (product)</p>'
      ]
    });

    mockToast = { add: jest.fn() };

    wrapper = shallowMount(ComplexMembers, {
      global: {
        components: { Dialog, Button, ProgressSpinner },
        mocks: { $toast: mockToast }
      },
      props: { conceptIri: "http://endhealth.info/im#CSET_BartsCVSSMeds", showDialog: true }
    });

    await flushPromises();
    jest.clearAllMocks();
  });

  it("calls getComplexMembers on mount and adds to variables", async () => {
    expect(wrapper.vm.loading).toBe(false);
    expect(wrapper.vm.complexMembers).toStrictEqual([
      '<p class="intersection">Intersection of</p><p class="name" style="margin-left: 40px;">UK product (product)</p><p class="having" style="margin-left: 40px;">having</p> <p class="role-group" style="margin-left: 80px">Has VMP (attribute)->COVID-19 vaccine (product)</p>'
    ]);
  });

  it("inits on conceptIri change", () => {
    wrapper.vm.init = jest.fn();
    wrapper.vm.$options.watch.conceptIri.call(wrapper.vm, "http://snomed.info/sct#92491000000104");
    expect(wrapper.vm.init).toHaveBeenCalledTimes(1);
  });

  it("gets complex members on init", () => {
    wrapper.vm.getComplexMembers = jest.fn();
    wrapper.vm.init();
    expect(wrapper.vm.getComplexMembers).toHaveBeenCalledTimes(1);
  });

  it("can get complex members", async () => {
    wrapper.vm.complexMembers = [];
    wrapper.vm.getComplexMembers();
    expect(wrapper.vm.loading).toBe(true);
    expect(EntityService.getComplexMembers).toHaveBeenCalledTimes(1);
    expect(EntityService.getComplexMembers).toHaveBeenCalledWith("http://endhealth.info/im#CSET_BartsCVSSMeds");
    await flushPromises();
    expect(wrapper.vm.complexMembers).toStrictEqual([
      '<p class="intersection">Intersection of</p><p class="name" style="margin-left: 40px;">UK product (product)</p><p class="having" style="margin-left: 40px;">having</p> <p class="role-group" style="margin-left: 80px">Has VMP (attribute)->COVID-19 vaccine (product)</p>'
    ]);
    expect(mockToast.add).not.toHaveBeenCalled();
    expect(wrapper.vm.loading).toBe(false);
  });

  it("can get complex members ___ fail", async () => {
    EntityService.getComplexMembers = jest.fn().mockRejectedValue(false);
    wrapper.vm.complexMembers = [];
    wrapper.vm.getComplexMembers();
    expect(wrapper.vm.loading).toBe(true);
    expect(EntityService.getComplexMembers).toHaveBeenCalledTimes(1);
    expect(EntityService.getComplexMembers).toHaveBeenCalledWith("http://endhealth.info/im#CSET_BartsCVSSMeds");
    await flushPromises();
    expect(wrapper.vm.complexMembers).toStrictEqual([]);
    expect(mockToast.add).toHaveBeenCalledTimes(1);
    expect(mockToast.add).toHaveBeenCalledWith(LoggerService.error("Failed to get complex members from server"));
    expect(wrapper.vm.loading).toBe(false);
  });

  it("can closeComplexMembersDialog", () => {
    wrapper.vm.closeComplexMembersDialog();
    expect(wrapper.emitted().closeComplexMembersDialog).toBeTruthy();
  });
});