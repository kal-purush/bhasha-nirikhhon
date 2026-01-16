package baekjoon.acmicpc.regional;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

// 12:10 ~ 01:02
public class Truck13335 {
    private class TruckOnBridge {
        int entranceTime;
        int weight;

        TruckOnBridge(int entranceTime, int weight) {
            this.entranceTime = entranceTime;
            this.weight = weight;
        }
    }

    private int mNumTrucks;
    private int mBridgeLength;
    private int mMaxLoad;
    private ArrayList<Integer> mTruckWeightList;

    public Truck13335() {
        init();
        System.out.println(getShortestTimePassingBridge());
    }

    public void init() {
        StringTokenizer tokenizer;
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            tokenizer = new StringTokenizer(reader.readLine());
            mNumTrucks = Integer.parseInt(tokenizer.nextToken());
            mBridgeLength = Integer.parseInt(tokenizer.nextToken());
            mMaxLoad = Integer.parseInt(tokenizer.nextToken());

            tokenizer = new StringTokenizer(reader.readLine());
            mTruckWeightList = new ArrayList<>(mNumTrucks);
            for (int i = 0; i < mNumTrucks; i++) {
                mTruckWeightList.add(Integer.parseInt(tokenizer.nextToken()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getShortestTimePassingBridge() {
        int shortestTime = 0;
        int load = 0;
        Queue<TruckOnBridge> bridge = new LinkedList<>();
        while (!mTruckWeightList.isEmpty() || !bridge.isEmpty()) {
            TruckOnBridge truck = bridge.peek();
            if (!bridge.isEmpty() && shortestTime - truck.entranceTime == mBridgeLength) {
                bridge.poll();
                load -= truck.weight;
            }
            if (!mTruckWeightList.isEmpty()) {
                int truckWeight = mTruckWeightList.get(0);
                if (load + truckWeight <= mMaxLoad) {
                    load += truckWeight;
                    bridge.offer(new TruckOnBridge(shortestTime, truckWeight));
                    mTruckWeightList.remove(0);
                }
            }
            shortestTime++;
        }
        return shortestTime;
    }
}