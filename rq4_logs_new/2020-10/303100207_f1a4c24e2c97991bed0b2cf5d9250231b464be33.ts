import {context} from "@actions/github"
import {OctokitClient} from '../../../clients/octokit-client'

describe("getRelease", () => {
    beforeEach(() => {
        jest.spyOn(context, "repo", "get").mockReturnValue({owner: "meclab-bot", repo: "special-octo-lamp"})
    })

    test("it should return null when no release exists and a 404 is returned", async () => {
        // arrange
        jest.spyOn(context, "repo", "get").mockReturnValue({owner: "meclab-bot", repo: "urban-doodle"})
        const sut = new OctokitClient(process.env.GITHUB_TOKEN!)

        // act
        const target = await sut.getRelease()

        // assert
        expect(target).toBeNull()
    })

    test("it should return the latest release when a release exists", async () => {
        // arrange
        const sut = new OctokitClient(process.env.GITHUB_TOKEN!)

        // act
        const target = await sut.getRelease()

        // assert
        expect(target).not.toBeNull()

        const {name, tag_name} = target!
        expect(name).toBe("v1")
        expect(tag_name).toBe("v1")
    })
})