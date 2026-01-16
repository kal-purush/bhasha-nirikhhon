import type { GroupRepository } from '../repositories/groupRepository'
import type { Group } from '../type'
import { GroupService } from './groupService'

const setup = () => {
  const groups: Group[] = [
    { name: 'テストグループ1', members: ['一郎', '二郎', '三郎'] },
    { name: 'テストグループ2', members: ['太郎', '花子'] },
  ]

  const group: Group = { name: 'テストグループ3', members: ['Alice', 'Bob'] }

  return {
    groups,
    group,
  }
}

describe('GroupService', () => {
  let mockGroupRepository: Partial<GroupRepository>
  let groupService: GroupService

  beforeEach(() => {
    mockGroupRepository = {
      loadGroups: jest.fn(),
      saveGroup: jest.fn(),
    }

    groupService = new GroupService(mockGroupRepository as GroupRepository)
  })

  describe('getGroups', () => {
    it('groupの配列が返却される', () => {
      const { groups } = setup()
      ;(mockGroupRepository.loadGroups as jest.Mock).mockReturnValueOnce(groups)
      const result = groupService.getGroups()
      expect(mockGroupRepository.loadGroups).toHaveBeenCalledWith()
      expect(result).toEqual(groups)
    })
  })

  describe('getGroupByName', () => {
    it('groupの配列が返却される', () => {
      const { groups } = setup()
      const spy = jest.spyOn(groupService, 'getGroups')
      spy.mockReturnValueOnce(groups)
      const result = groupService.getGroupByName('テストグループ1')
      expect(spy).toHaveBeenCalledWith()
      expect(result).toEqual(groups[0])
    })
  })

  describe('addGroup', () => {
    it('groupが保存される', () => {
      const { groups, group } = setup()
      const result = [...groups, group]
      ;(mockGroupRepository.saveGroup as jest.Mock).mockImplementationOnce(
        () => {
          groups.push(group)
        },
      )
      groupService.addGroup(group)
      expect(mockGroupRepository.saveGroup).toHaveBeenCalledWith(group)
      expect(result).toEqual(groups)
    })
  })
})