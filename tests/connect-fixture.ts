import {
	create,
	createFileRegistry,
	type DescMessage,
	type DescMethodServerStreaming,
} from "@bufbuild/protobuf";
import { FileDescriptorSetSchema } from "@bufbuild/protobuf/wkt";

const registry = createFileRegistry(
	create(FileDescriptorSetSchema, {
		file: [
			{
				name: "watch.proto",
				package: "example",
				syntax: "proto3",
				messageType: [
					{
						name: "Request",
						field: [{ name: "id", jsonName: "id", number: 1, type: 9 }],
					},
					{
						name: "Response",
						field: [{ name: "value", jsonName: "value", number: 1, type: 5 }],
					},
				],
				service: [
					{
						name: "Status",
						method: [
							{
								name: "Watch",
								inputType: ".example.Request",
								outputType: ".example.Response",
								serverStreaming: true,
							},
						],
					},
				],
			},
		],
	}),
);
export const method = registry.getService("example.Status")
	?.methods[0] as DescMethodServerStreaming<DescMessage, DescMessage>;
