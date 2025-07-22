use anyhow::{Context, Result};
use std::time::Duration;
use tokio::time::interval;
use tracing::{info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    subscribe_request::{Accounts, Transactions},
    CommitmentLevel, SubscribeRequest,
};


// 配置信息 - 你需要修改这里的内容
const GRPC_ENDPOINT: &str = "https://api.rpcpool.com:443";

const AUTH_TOKEN: &str = "token";
const PING_INTERVAL_SECS: u64 = 30; // 每30秒发送一次心跳保持连接


#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志系统，
    tracing_subscriber::fmt::init();
    info!("Solana gRPC 监听程序启动");

    // 连接到 gRPC 服务器
    info!("正在连接到 {}", GRPC_ENDPOINT);
    let mut client = GeyserGrpcClient::connect(GRPC_ENDPOINT)
        .await
        .context("连接服务器失败")?;

    // 设置认证信息
    client.set_auth_token(AUTH_TOKEN);

    // 创建数据订阅流
    info!("正在创建数据订阅...");
    let (mut sender, mut receiver) = client.subscribe().await
        .context("创建订阅失败")?;

    // 定义我们想要监听的数据类型和条件
    let subscribe_request = SubscribeRequest {
        // 监听特定账户 - 这里监听USDC代币账户
        accounts: Some(Accounts {
            filters: vec![
                yellowstone_grpc_proto::geyser::AccountFilter {
                    // USDC代币的账户地址
                    account: vec!["9wFFyRfZBsuAha4YcuxcXLKwMxJR43S7fPfQLusDBzvT".to_string()],
                    owner: vec![], // 不按所有者过滤
                    ..Default::default()
                }
            ],
            ..Default::default()
        }),
        // 监听交易 - 只关注成功的非投票交易
        transactions: Some(Transactions {
            filters: vec![
                yellowstone_grpc_proto::geyser::TransactionFilter {
                    vote: Some(false), // 排除投票交易
                    failed: Some(false), // 排除失败交易
                    ..Default::default()
                }
            ],
            ..Default::default()
        }),
        // 监听区块插槽更新
        slots: Some(()),
        // 数据确认级别：Confirmed表示已确认的数据
        commitment: CommitmentLevel::Confirmed as i32,
        ..Default::default()
    };

    // 发送订阅请求
    sender.send(subscribe_request).await
        .context("发送订阅请求失败")?;
    info!("订阅请求已发送，开始接收数据...");

    // 启动心跳任务，定期发送信号保持连接
    let mut ping_interval = interval(Duration::from_secs(PING_INTERVAL_SECS));
    let mut sender_clone = sender.clone();
    tokio::spawn(async move {
        loop {
            ping_interval.tick().await;
            let ping_request = SubscribeRequest {
                ping: Some(yellowstone_grpc_proto::geyser::Ping {
                    id: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as u64,
                }),
                ..Default::default()
            };
            
            if let Err(e) = sender_clone.send(ping_request).await {
                warn!("发送心跳失败: {}", e);
                break;
            }
            info!("发送了心跳信号，保持连接活跃");
        }
    });

    // 循环接收并处理数据
    while let Some(msg) = receiver.message().await.context("接收数据失败")? {
        // 处理账户更新数据
        if let Some(accounts) = msg.accounts {
            for account in accounts.accounts {
                info!("\n=== 检测到账户更新 ===");
                info!("账户地址: {}", account.pubkey);
                info!("所在区块: {}", account.slot);
                info!("数据长度: {} 字节", account.data.len());
            }
        }

        // 处理交易数据
        if let Some(transactions) = msg.transactions {
            for tx in transactions.transactions {
                info!("\n=== 检测到新交易 ===");
                info!("交易签名: {}", tx.signature);
                info!("所在区块: {}", tx.slot);
                info!("是否成功: {}", tx.success);
                info!("交易费用: {}", tx.fee);
            }
        }

        // 处理区块插槽更新
        if let Some(slots) = msg.slots {
            for slot in slots.slots {
                info!("\n=== 区块插槽更新 ===");
                info!("插槽编号: {}", slot.slot);
                info!("父插槽: {}", slot.parent);
                info!("状态: {:?}", slot.status);
            }
        }

        // 处理心跳响应
        if let Some(pong) = msg.pong {
            info!("收到服务器回应，连接正常 (ID: {})", pong.id);
        }
    }

    info!("结束");
    Ok(())
}
