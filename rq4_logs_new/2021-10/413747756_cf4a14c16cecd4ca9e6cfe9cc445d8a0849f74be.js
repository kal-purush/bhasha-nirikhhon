import React from "react";

const Community = () => {
  return (
    <div className="bg-grey-bg px-10" id="community">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-12">
        <img src="/images/Community_Text.png" />

        <div className="grid grid-cols-1 lg:grid-cols-3">
          <div className="col-span-2 text-left text-white">
            <h1 className="font-bold text-4xl mt-4">1.</h1>
            <p>
              We believe that our NFT Realtors deserve the best and that’s is
              why we will give back A LOT For example, when we reach 30% minted
              NFT’s we will Airdrop 25 NFT Realtor Licenses. The holders of
              these licenses will get 50% of the royalties from secondary sales!
              Royalties will be set at 3% for secondary sales. We will announce
              many more during and after launch. So again, stay tuned and join
              our Discord channel.
            </p>

            <h1 className="font-bold text-4xl mt-4">2.</h1>
            <p>
              We give back to the community who isn’t that lucky. We’ll give a
              homeless mother/father with children a 1-year rent + gift card for
              food. 2 holders will get their NFT in an artwork done by Richard
              Hutchins (Homeless Artist) and many more.
            </p>

            <h1 className="font-bold text-4xl mt-4">3.</h1>
            <p>
              We have a lot of interesting additional benefits & giveaways
              prepared for our members, that will be unlocked through roadmap
              activations and future phases.
            </p>
          </div>
          <div>
            <img src="/images/community.png" />
          </div>
        </div>
      </div>
    </div>
  );
};

export default Community;